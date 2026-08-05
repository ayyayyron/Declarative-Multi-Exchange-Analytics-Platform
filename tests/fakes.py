from common.models import DeadLetterEvent, TradeEvent


class RecordingPublisher:
    def __init__(self) -> None:
        self.trades: list[TradeEvent] = []
        self.dead_letters: list[DeadLetterEvent] = []

    def publish_trade(self, event: TradeEvent) -> None:
        self.trades.append(event)

    def publish_dead_letter(self, event: DeadLetterEvent) -> None:
        self.dead_letters.append(event)
