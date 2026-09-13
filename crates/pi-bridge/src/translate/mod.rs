pub mod events;
pub mod input;
pub mod items;
pub mod tool_call;

use crate::codex_proto::{TurnError, TurnStatus};
use crate::pool::pi_protocol::StopReason;

pub(crate) fn turn_terminal_state(
    stop_reason: Option<StopReason>,
    error_message: Option<&str>,
) -> (TurnStatus, Option<TurnError>) {
    let message = error_message
        .map(str::trim)
        .filter(|message| !message.is_empty());
    match stop_reason {
        Some(StopReason::Aborted) => (
            TurnStatus::Interrupted,
            message.map(|message| TurnError {
                message: message.to_string(),
                codex_error_info: None,
                additional_details: None,
            }),
        ),
        Some(StopReason::Error) => (
            TurnStatus::Failed,
            Some(TurnError {
                message: message.unwrap_or("Pi turn failed").to_string(),
                codex_error_info: None,
                additional_details: None,
            }),
        ),
        Some(StopReason::Stop | StopReason::Length | StopReason::ToolUse) => {
            (TurnStatus::Completed, None)
        }
        None if message.is_some() => (
            TurnStatus::Failed,
            Some(TurnError {
                message: message.unwrap().to_string(),
                codex_error_info: None,
                additional_details: None,
            }),
        ),
        None => (TurnStatus::Completed, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_state_preserves_pi_stop_reasons() {
        for reason in [StopReason::Stop, StopReason::Length, StopReason::ToolUse] {
            let (status, error) = turn_terminal_state(Some(reason), Some("ignored"));
            assert_eq!(status, TurnStatus::Completed);
            assert!(error.is_none());
        }

        let (status, error) =
            turn_terminal_state(Some(StopReason::Error), Some("  model failed  "));
        assert_eq!(status, TurnStatus::Failed);
        assert_eq!(error.unwrap().message, "model failed");

        let (status, error) = turn_terminal_state(Some(StopReason::Aborted), None);
        assert_eq!(status, TurnStatus::Interrupted);
        assert!(error.is_none());
    }
}
