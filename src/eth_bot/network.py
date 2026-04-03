from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import NetworkConfig
from .models import NetworkScores


def _relu(value: float) -> float:
    return value if value > 0 else 0.0


def _relu_derivative(value: float) -> float:
    return 1.0 if value > 0 else 0.0


def _sigmoid(value: float) -> float:
    if value >= 0:
        exp = math.exp(-value)
        return 1.0 / (1.0 + exp)
    exp = math.exp(value)
    return exp / (1.0 + exp)


@dataclass
class NeuralNetwork:
    config: NetworkConfig
    weights: list[list[list[float]]]
    biases: list[list[float]]
    version: str

    @classmethod
    def random(cls, config: NetworkConfig, *, seed: int | None = None, version: str | None = None) -> "NeuralNetwork":
        config.validate()
        rng = random.Random(config.seed if seed is None else seed)
        weights: list[list[list[float]]] = []
        biases: list[list[float]] = []
        for input_size, output_size in zip(config.layer_sizes[:-1], config.layer_sizes[1:]):
            scale = 1 / math.sqrt(max(1, input_size))
            layer_weights = [
                [rng.uniform(-scale, scale) for _ in range(input_size)]
                for _ in range(output_size)
            ]
            layer_biases = [rng.uniform(-scale, scale) for _ in range(output_size)]
            weights.append(layer_weights)
            biases.append(layer_biases)
        return cls(config=config, weights=weights, biases=biases, version=version or config.version)

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "NeuralNetwork":
        config_payload = payload.get("config", {})
        layer_sizes = tuple(int(value) for value in config_payload.get("layer_sizes", (24, 32, 24, 16, 8, 2)))
        config = NetworkConfig(
            layer_sizes=layer_sizes,
            learning_rate=float(config_payload.get("learning_rate", 0.01)),
            seed=int(config_payload.get("seed", 7)),
            mutation_scale=float(config_payload.get("mutation_scale", 0.05)),
            version=str(config_payload.get("version", payload.get("version", "baseline-v1"))),
        )
        return cls(
            config=config,
            weights=[
                [[float(weight) for weight in neuron] for neuron in layer]
                for layer in payload["weights"]
            ],
            biases=[[float(value) for value in layer] for layer in payload["biases"]],
            version=str(payload.get("version", config.version)),
        )

    @classmethod
    def load_or_create(
        cls,
        path: Path,
        config: NetworkConfig,
        *,
        baseline: "NeuralNetwork" | None = None,
        mutation_scale: float | None = None,
        seed_offset: int = 0,
    ) -> "NeuralNetwork":
        if path.exists():
            return cls.from_json(json.loads(path.read_text(encoding="utf-8")))
        if baseline is None:
            network = cls.random(config, seed=config.seed + seed_offset)
        else:
            network = baseline.mutate(scale=mutation_scale or 0.0, seed=config.seed + seed_offset)
            network.version = baseline.version
        network.save(path)
        return network

    def to_json(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "config": {
                "layer_sizes": list(self.config.layer_sizes),
                "learning_rate": self.config.learning_rate,
                "seed": self.config.seed,
                "mutation_scale": self.config.mutation_scale,
                "version": self.config.version,
            },
            "weights": self.weights,
            "biases": self.biases,
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_json(), indent=2), encoding="utf-8")

    def mutate(self, *, scale: float | None = None, seed: int | None = None) -> "NeuralNetwork":
        rng = random.Random(self.config.seed if seed is None else seed)
        actual_scale = self.config.mutation_scale if scale is None else scale
        weights = [
            [
                [weight + rng.gauss(0.0, actual_scale) for weight in neuron]
                for neuron in layer
            ]
            for layer in self.weights
        ]
        biases = [
            [value + rng.gauss(0.0, actual_scale) for value in layer]
            for layer in self.biases
        ]
        return NeuralNetwork(config=self.config, weights=weights, biases=biases, version=self.version)

    def _forward_pass(self, features: list[float]) -> tuple[list[list[float]], list[list[float]]]:
        activations: list[list[float]] = [list(features)]
        z_values: list[list[float]] = []
        current = list(features)

        for layer_index, (layer_weights, layer_biases) in enumerate(zip(self.weights, self.biases)):
            z_layer: list[float] = []
            next_layer: list[float] = []
            is_output_layer = layer_index == len(self.weights) - 1
            for neuron_weights, bias in zip(layer_weights, layer_biases):
                z_value = sum(weight * activation for weight, activation in zip(neuron_weights, current)) + bias
                z_layer.append(z_value)
                next_layer.append(_sigmoid(z_value) if is_output_layer else _relu(z_value))
            z_values.append(z_layer)
            activations.append(next_layer)
            current = next_layer
        return activations, z_values

    def forward(self, features: list[float]) -> NetworkScores:
        activations, _ = self._forward_pass(features)
        outputs = activations[-1]
        hidden_activations = [layer[:] for layer in activations[1:-1]]
        return NetworkScores(
            prob_win_long=float(outputs[0]),
            prob_win_short=float(outputs[1]),
            hidden_activations=hidden_activations,
            version=self.version,
        )

    def train(
        self,
        samples: list[dict[str, Any]],
        *,
        epochs: int = 10,
        label_key: str = "label_win_fee_aware",
    ) -> dict[str, Any]:
        if not samples:
            return {"samples": 0, "epochs": epochs, "average_loss": 0.0}

        learning_rate = self.config.learning_rate
        total_loss = 0.0
        total_observations = 0

        for _ in range(epochs):
            for sample in samples:
                features = [float(value) for value in sample.get("entry_features", [])]
                if len(features) != self.config.layer_sizes[0]:
                    continue

                target = [0.0, 0.0]
                mask = [0.0, 0.0]
                side = str(sample.get("side", "")).lower()
                label_value = float(sample.get(label_key, sample.get("label_win_fee_aware", 0)))
                if side == "long":
                    target[0] = label_value
                    mask[0] = 1.0
                elif side == "short":
                    target[1] = label_value
                    mask[1] = 1.0
                else:
                    continue

                activations, z_values = self._forward_pass(features)
                outputs = activations[-1]
                loss = 0.0
                for index in range(2):
                    if mask[index] == 0:
                        continue
                    prediction = min(max(outputs[index], 1e-7), 1 - 1e-7)
                    loss += -(
                        target[index] * math.log(prediction)
                        + (1 - target[index]) * math.log(1 - prediction)
                    )
                total_loss += loss
                total_observations += int(sum(mask))

                deltas: list[list[float]] = [[] for _ in self.weights]
                deltas[-1] = [
                    (output - target[index]) * mask[index]
                    for index, output in enumerate(outputs)
                ]

                for layer_index in range(len(self.weights) - 2, -1, -1):
                    layer_delta: list[float] = []
                    for neuron_index in range(len(self.weights[layer_index])):
                        downstream = 0.0
                        for downstream_index, downstream_weights in enumerate(self.weights[layer_index + 1]):
                            downstream += downstream_weights[neuron_index] * deltas[layer_index + 1][downstream_index]
                        layer_delta.append(downstream * _relu_derivative(z_values[layer_index][neuron_index]))
                    deltas[layer_index] = layer_delta

                for layer_index, layer_weights in enumerate(self.weights):
                    previous_activations = activations[layer_index]
                    for neuron_index, neuron_weights in enumerate(layer_weights):
                        delta = deltas[layer_index][neuron_index]
                        for weight_index in range(len(neuron_weights)):
                            neuron_weights[weight_index] -= learning_rate * delta * previous_activations[weight_index]
                        self.biases[layer_index][neuron_index] -= learning_rate * delta

        average_loss = total_loss / total_observations if total_observations else 0.0
        return {
            "samples": len(samples),
            "epochs": epochs,
            "average_loss": average_loss,
        }


def load_training_samples(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    samples: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            samples.append(json.loads(line))
    return samples
