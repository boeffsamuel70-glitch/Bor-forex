# Bot Forex 5M — versão corrigida

Correções:
- primeira leitura imediatamente após o deploy;
- logs detalhados no Render;
- leitura dos 3 ativos;
- novas leituras cerca de 1 minuto após cada fechamento de candle de 5 minutos;
- chave da Twelve Data somente por variável de ambiente.

Render:
Build Command: `pip install -r requirements.txt`
Start Command: `gunicorn --workers 1 app:app`
Environment Variable: `TWELVE_DATA_API_KEY` = sua chave NOVA da Twelve Data.

Não coloque a chave diretamente no código.
