# Bot Forex 5M — versão atualidade v2

## Correções
- Bloqueia CALL/PUT se o último candle recebido tiver mais de 8 minutos.
- Ordena os candles cronologicamente antes da análise.
- Usa sempre o candle de 5 minutos **já fechado mais recente** para o sinal.
- Exibe a idade do dado no painel.
- Registra no log o último candle recebido e o candle fechado usado na análise.

## Render
Build Command:
`pip install -r requirements.txt`

Start Command:
`gunicorn --workers 1 app:app`

## Environment
`TWELVE_DATA_API_KEY` = sua chave da Twelve Data.

Não coloque a chave diretamente no código.
