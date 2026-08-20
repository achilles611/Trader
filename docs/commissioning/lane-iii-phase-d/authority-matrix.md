# L3-D authority matrix

| Capability / Authority | L3-D |
| --- | --- |
| Consume L3-C hypothesis state | YES |
| Evaluate frozen Trader V0 policy | YES |
| Produce `NO_TRADE` | YES |
| Produce `LONG` signal | YES |
| Produce `SHORT` signal | YES |
| Produce `EXIT` signal | YES |
| Maintain strategy decision lifecycle | YES |
| Use confidence as relative support | YES |
| Treat confidence as win probability | NO |
| Recompute raw market evidence | NO |
| Arbitrarily size positions | NO |
| Create broker orders | NO |
| Create final execution intents | NO |
| Override hard risk | NO |
| Override operator flatten | NO |
| Contact broker | NO |
| Contact prop account | NO |
| Control copier | NO |
| Modify Phase E | NO |
| Self-optimize | NO |
| Live-capital authority | NO |

The exact semantic artifact grants only directional signal authority. Static
tests verify no Lane II, Phase D/E/F, copy-trading, broker SDK, or network
dependency and no execution/sizing/account method or signal field.
