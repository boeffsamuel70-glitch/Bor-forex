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


## v3 — correção do /dados e do processo Gunicorn

Esta versão corrige um problema de arquitetura: o loop do robô estava sendo iniciado durante o import do `app.py`. Com Gunicorn, isso podia acontecer no processo mestre, enquanto `/dados` era atendido por um worker separado, fazendo o painel continuar em `AGUARDAR / Score 0 / Preço -`.

Agora o robô é iniciado dentro do próprio worker que atende as requisições web. Assim, o estado usado pelo loop e o estado retornado por `/dados` ficam no mesmo processo.

Também foram reforçados:
- ordenação cronológica dos candles;
- seleção da última vela fechada;
- bloqueio de dados com atraso superior a 8 minutos;
- exibição da idade do dado no painel;
- logs de `último candle`, `atraso` e `vela fechada`;
- agendamento na próxima fronteira de 5 minutos com pequeno buffer.
