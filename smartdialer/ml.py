import hashlib
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path


def stable_uniform(*parts):
    digest = hashlib.blake2b(repr(parts).encode(), digest_size=8).digest()
    return int.from_bytes(digest, 'big') / 2**64


def sigmoid(value):
    return 1 / (1 + math.exp(-max(-35, min(35, value))))


def features(now, past_calls, past_answers, attempts):
    # Use the campaign's fixed UTC+05:30 clock in training and inference.
    angle = ((now + 19800) % 86400) * math.tau / 86400
    return (1.0, math.sin(angle), math.cos(angle), (past_answers + 1) / (past_calls + 2),
            min(attempts, 5) / 5)


def answer_probability(row, base_rate):
    base = min(0.999, max(0.001, base_rate))
    log_odds = math.log(base / (1 - base))
    return sigmoid(log_odds + 0.65 * row[1] - 0.55 * row[2]
                   + 5.0 * (row[3] - 0.5) - 0.6 * row[4])


@dataclass(frozen=True)
class AnswerModel:
    weights: tuple

    def score(self, now, borrower):
        row = features(now, borrower['past_calls'], borrower['past_answers'], borrower['attempts'])
        return self.predict(row)

    def predict(self, row):
        return sigmoid(sum(weight * value for weight, value in zip(self.weights, row)))

    def save(self, path):
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps({'version': 1, 'weights': self.weights}, indent=2) + '\n',
                          encoding='utf-8')

    @classmethod
    def load(cls, path):
        if not path:
            return None
        try:
            payload = json.loads(Path(path).read_text(encoding='utf-8'))
            weights = payload['weights']
            if (type(payload['version']) is int and payload['version'] == 1 and isinstance(weights, list)
                    and len(weights) == 5 and all(type(value) in (int, float) and math.isfinite(value) for value in weights)):
                return cls(tuple(float(value) for value in weights))
        except (OSError, ValueError, TypeError, KeyError, OverflowError):
            pass
        return None


def _synthetic_split(seed):
    rng = random.Random(seed)
    data = []
    for index in range(2600):
        now = 1_750_000_000 + index * 900
        history_answers = rng.randrange(13)
        row = features(now, 12, history_answers, rng.randrange(3))
        label = int(rng.random() < answer_probability(row, 0.5))
        data.append((row, label))
    return data[:2000], data[2000:]


def _evaluate_split(model, training, held_out):
    base_rate = sum(label for _, label in training) / len(training)
    return {
        'training_rows': len(training), 'held_out_rows': len(held_out),
        'held_out_brier': round(sum((model.predict(row) - label) ** 2 for row, label in held_out)
                                / len(held_out), 4),
        'constant_baseline_brier': round(sum((base_rate - label) ** 2 for _, label in held_out)
                                         / len(held_out), 4),
    }


def evaluate(model, seed=17):
    return _evaluate_split(model, *_synthetic_split(seed))


def train(seed=17):
    training, held_out = _synthetic_split(seed)
    weights = [0.0] * 5
    for _ in range(250):
        gradient = [0.0] * len(weights)
        for row, label in training:
            error = sigmoid(sum(weight * value for weight, value in zip(weights, row))) - label
            for index, value in enumerate(row):
                gradient[index] += error * value
        for index, value in enumerate(gradient):
            weights[index] -= 1.1 * value / len(training)
    model = AnswerModel(tuple(weights))
    return model, _evaluate_split(model, training, held_out)
