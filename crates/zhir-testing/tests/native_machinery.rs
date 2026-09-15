//! Exercise internal mechanisms from their production source without exporting
//! implementation helpers in the public SDK or shipping test code in its crates.
#[path = "../../zhir-models/src/native/confirmation.rs"]
mod confirmation;
#[path = "../../zhir-models/src/native/media.rs"]
mod media;
#[path = "../../zhir-models/src/transport/rtp.rs"]
mod rtp;
use std::time::Duration;
use zhir_core::error::Error;

#[tokio::test(start_paused = true)]
async fn confirmation_cannot_be_overwritten_or_acknowledged_after_timeout() {
    let mut pending = confirmation::Confirmation::default();
    pending
        .begin("first", "ready", Duration::from_secs(1))
        .unwrap();
    let deadline = pending.deadline();
    assert!(matches!(
        pending.begin("second", "ready", Duration::from_secs(2)),
        Err(Error::Protocol(_))
    ));
    assert_eq!(pending.pending(), Some(&"first"));
    assert_eq!(pending.deadline(), deadline);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        matches!(pending.complete(), Err(Error::Uncertain(message)) if message.contains("ready") && message.contains("first"))
    );
    assert_eq!(
        pending.pending(),
        Some(&"first"),
        "timeout must not settle a command"
    );
}

#[tokio::test]
async fn confirmed_slot_rejects_duplicate_confirmation() {
    let mut pending = confirmation::Confirmation::default();
    for command in ["pause", "resume"] {
        pending
            .begin(command, "ready", Duration::from_secs(1))
            .unwrap();
        assert_eq!(pending.complete().unwrap(), command);
        assert!(pending.deadline().is_none());
        assert!(matches!(pending.complete(), Err(Error::Protocol(_))));
    }
}

#[test]
fn rtp_wrap_loss_duplicates_and_reordering_preserve_the_timeline() {
    let mut timeline = rtp::RtpTimeline::default();
    assert_eq!(timeline.accept(9, 65534, u32::MAX - 959).unwrap(), Some(0));
    assert_eq!(timeline.accept(9, 0, 960).unwrap(), Some(1920));
    assert_eq!(timeline.accept(9, 65535, 0).unwrap(), None);
    assert_eq!(timeline.accept(9, 0, 960).unwrap(), None);
    assert_eq!(timeline.accept(9, 1, 1920).unwrap(), Some(2880));
    assert!(matches!(
        timeline.accept(10, 2, 2880),
        Err(Error::Uncertain(_))
    ));
    assert!(matches!(
        timeline.accept(9, 2, 1000),
        Err(Error::Protocol(_))
    ));
    assert_eq!(timeline.ticks(), 2880);
}

#[tokio::test]
async fn media_budget_follows_packets_until_consumption() {
    let budget = media::MediaBudget::new(3);
    let packet = media::Buffered {
        value: vec![1, 2, 3],
        reservation: budget.reserve(3).await.unwrap(),
    };
    // A zero-byte final marker must fit even if payload bytes fill the budget.
    let end = budget.reserve(0).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), budget.reserve(1))
            .await
            .is_err()
    );
    #[cfg(feature = "webrtc")]
    let packet = {
        assert!(matches!(budget.try_reserve(1), Err(Error::Uncertain(_))));
        packet.map(|bytes| ("RTP to MediaChunk", bytes))
    };
    let _value = packet.into_inner();
    let bytes = budget.reserve(3).await.unwrap();
    assert!(matches!(budget.reserve(4).await, Err(Error::Invalid(_))));
    drop((bytes, end));
    budget.reserve(3).await.unwrap();
}

#[tokio::test]
async fn zero_byte_media_still_has_a_finite_slot_budget() {
    let budget = media::MediaBudget::new(1);
    let mut markers = vec![];
    for _ in 0..media::MAX_BUFFERED_CHUNKS {
        markers.push(budget.reserve(0).await.unwrap());
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(10), budget.reserve(0))
            .await
            .is_err()
    );
    drop(markers);
    budget.reserve(1).await.unwrap();
}
