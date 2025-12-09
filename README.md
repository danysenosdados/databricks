<h1 align="center"> Pipeline de Dados - TransfereGov (Databricks)</h1>

<p align="center">
  <strong>Arquitetura Bronze → Silver → Gold</strong><br>
  Projeto desenvolvido na plataforma Databricks Community Edition<br>
  com orquestração via Scheduler manual e processamento PySpark.
</p>

<hr>

<h2>📌 Visão Geral</h2>

<p>
Este repositório contém um pipeline de ingestão, transformação e modelagem baseado nos dados públicos do <strong>TransfereGov</strong>.  
Adota-se a clássica arquitetura <strong>Bronze → Silver → Gold</strong>, com armazenamento em tabelas Delta e organização modular por endpoints.
</p>

<ul>
  <li><strong>Bronze:</strong> Captura direta das APIs em JSON bruto.</li>
  <li><strong>Silver:</strong> Padronização, limpeza, tipagem e normalização das colunas.</li>
  <li><strong>Gold:</strong> Aplicação de regras de negócio, filtros específicos, métricas finais e entrega para consumo analítico.</li>
</ul>

<hr>

<h2>🏛️ Arquitetura</h2>

<h3>Bronze</h3>
<p>
A fase Bronze é responsável por:
</p>
<ul>
  <li>Conectar aos endpoints oficiais do TransfereGov.</li>
  <li>Persistir o JSON bruto integral.</li>
  <li>Controlar metadados como data de execução e paginação.</li>
</ul>

<p>
Formato de escrita: <strong>Delta Lake</strong><br>
Modo: <code>append</code>
</p>

<hr>

<h3>Silver</h3>
<p>
Nesta camada ocorre a <strong>transformação estrutural</strong>:
</p>

<ul>
  <li>Normalização de nomes de colunas.</li>
  <li>Conversão de tipos (inteiro, double, timestamp).</li>
  <li>Tratamento de valores nulos e inconsistências.</li>
</ul>

<p>
Formato de escrita: <strong>Delta Lake</strong><br>
Modo: <code>overwrite</code> com <code>overwriteSchema=true</code>
</p>

<hr>

<h3>Gold</h3>

<p>A camada Gold refina e aplica regras de negócio específicas. Cada endpoint possui sua própria regra. A seguir, os scripts principais implementados:</p>

<h4>1️⃣ gold.transferegov.plano_acao</h4>

<p>
Regras aplicadas:
</p>

<ul>
  <li>Carregamento da tabela <code>silver.transferegov.plano_acao</code>.</li>
  <li>Filtro de CNPJs autorizados</li>
  <li>Persistência final em <code>gold.transferegov.plano_acao</code>.</li>
</ul>

<hr>

<h4>2️⃣ gold.transferegov.programa_beneficiario</h4>

<p>Mesma estrutura, porém aplicando o filtro sobre a coluna:</p>

<pre><code>cnpj_beneficiario_programa</code></pre>

<ul>
  <li>Leitura da tabela Silver.</li>
  <li>Aplicação da regra de CNPJs.</li>
  <li>Escrita final em <code>gold.transferegov.programa_beneficiario</code>.</li>
</ul>

<hr>

<h2>⚙️ Tecnologias Utilizadas</h2>

<ul>
  <li>Apache Spark / PySpark</li>
  <li>Delta Lake</li>
  <li>Databricks Community Edition</li>
  <li>Scheduler manual para orquestração</li>
  <li>APIs REST do TransfereGov</li>
</ul>

<hr>

<h2>📂 Estrutura de Pastas</h2>

<pre>
/bronze
  ingest_<endpoint>.py

/silver
  silver_<endpoint>.py

/gold
  gold_<endpoint>.py
</pre>

<hr>

<h2>🚀 Como Executar</h2>

<ol>
  <li>Carregar os scripts no Workspace do Databricks.</li>
  <li>Executar manualmente cada fase ou agendar via Scheduler.</li>
  <li>Garantir que os endpoints e widgets estejam configurados.</li>
</ol>

<hr>

<h2>📜 Licença</h2>

<p>Uso livre para fins acadêmicos e profissionais.</p>
