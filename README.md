# Bot Forex 5M — Render + GitHub + Twelve Data

Ativos:
- EUR/USD
- GBP/USD
- EUR/JPY

Timeframe:
- 5 minutos

Estratégia:
- EMA 5/13/21
- RSI 14
- ATR 14
- força do candle
- momentum
- filtros de lateralização/baixa volatilidade
- confluência mínima para liberar CALL/PUT

## Segurança

NUNCA coloque a chave da Twelve Data dentro do `app.py` ou no GitHub.

No Render, crie a variável:

`TWELVE_DATA_API_KEY`

Use uma chave nova, porque a chave anteriormente compartilhada em conversa deve ser considerada comprometida.

## Render

Build Command:

`pip install -r requirements.txt`

Start Command:

`gunicorn --workers 1 app:app`

O worker único evita iniciar múltiplas cópias do loop de coleta.

## Limite do plano gratuito

Este projeto foi ajustado para 3 ativos consultados a cada 5 minutos somente das 06:00 às 22:00, de segunda a sexta.

Isso fica abaixo dos 800 créditos/dia do plano Basic gratuito da Twelve Data.

IMPORTANTE: a API REST pode entregar o candle recém-fechado com algum atraso. Portanto, este projeto não promete entrada exatamente no segundo do fechamento da vela. Para opções binárias, valide a diferença entre a cotação da Twelve Data e a cotação da sua corretora.

O score exibido é uma pontuação de confluência, NÃO é probabilidade real de acerto.

## Arquivos

- `app.py`
- `requirements.txt`
- `.gitignore`
