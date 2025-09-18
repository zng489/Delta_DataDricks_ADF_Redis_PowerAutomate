# Databricks notebook source
# Ensure the necessary package is installed
# %pip install --upgrade typing_extensions

from openai import OpenAI
import os

# Set up your Databricks API token
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"  # Use your actual token here or load it securely from env

# Initialize the OpenAI client with the Databricks token and custom base URL
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://adb-6523536500265509.9.azuredatabricks.net/serving-endpoints"
)

# First LLM for initial response (e.g., summarizing or explaining)
llm_1_response = client.chat.completions.create(
    messages=[
        {"role": "system", "content": "You are an AI assistant."},
        {"role": "user", "content": "Tell me about this dataframe zhang"}
    ],
    model="databricks-meta-llama-3-1-405b-instruct",  # Model 1
    max_tokens=256
)

# Extract response from LLM 1
llm_1_output = llm_1_response.choices[0].message['content']
print(f"LLM 1 Response (Initial): {llm_1_output}")

# Second LLM for refining or enhancing the response (e.g., making it more detailed)
llm_2_response = client.chat.completions.create(
    messages=[
        {"role": "system", "content": "You are an assistant refining responses."},
        {"role": "user", "content": f"Enhance this explanation: {llm_1_output}"}
    ],
    model="databricks-meta-llama-3-1-70b-instruct",  # Model 2 (for refining)
    max_tokens=256
)

# Extract and print the refined output
llm_2_output = llm_2_response.choices[0].message['content']
print(f"LLM 2 Response (Refined): {llm_2_output}")


# COMMAND ----------

from langchain.chains import SimpleChain
from langchain.llms import OpenAI, HuggingFaceLLM

# Carregar dois modelos LLM
llm_1 = OpenAI(model="text-davinci-003")  # Usando um modelo GPT
llm_2 = HuggingFaceLLM(model="t5-small")  # Usando um modelo T5 para tarefas específicas

# Definir as funções de cada LLM
def generate_summary(text):
    return llm_1.predict(f"Summarize the following text: {text}")

def refine_summary(summary):
    return llm_2.predict(f"Refine this summary for clarity and detail: {summary}")

# Criar um fluxo de trabalho encadeado
def process_text(text):
    summary = generate_summary(text)  # Passo 1: Gerar o resumo
    refined_summary = refine_summary(summary)  # Passo 2: Refinar o resumo
    return refined_summary

# Usar o fluxo de trabalho
input_text = "LangChain é uma biblioteca poderosa que facilita o uso de LLMs em diversos cenários."
output = process_text(input_text)
print(output)


# COMMAND ----------

Resumo das Semelhanças e Diferenças:
Tecnologia	Semelhanças	Diferenças
Agente de IA	Pode integrar LLMs ou outras tecnologias para realizar ações de maneira autônoma.	Foco principal em ação no ambiente, e não apenas em linguagem ou geração de texto.
LLM	Usado em várias aplicações de IA, incluindo agentes de IA, para interação com linguagem.	Focado em linguagem (geração ou compreensão), e não em interação com o ambiente físico.
RAG	Geração de texto e uso de informações externas para aprimorar a resposta.	Combinando recuperação de dados + geração, enquanto LLM gera texto sem dados externos em tempo real.
LangChain	Facilita a criação de aplicações de IA integrando LLMs, APIs, e fontes externas.	Uma ferramenta para estruturar o uso de LLMs, não um modelo ou técnica de IA em si.
Esses termos estão interligados, mas cada um tem um papel específico na construção de sistemas mais avançados de IA. Em resumo:

Agentes de IA atuam no ambiente;
LLMs se concentram em gerar ou entender linguagem;
RAG é uma técnica que completa os LLMs com informações externas;
LangChain ajuda a organizar e integrar tudo isso em aplicações completas.
Espero que tenha ficado claro! Se precisar de mais detalhes sobre algum deles, é só avisar.

Você disse:
o que mais novos termos podem ser citados
O ChatGPT disse:
Além dos termos que você mencionou, há diversos outros conceitos e termos que têm surgido com o avanço da inteligência artificial, especialmente no campo de Modelos de Linguagem, Aprendizado de Máquina e Geração de Conteúdo. Aqui estão alguns dos mais novos e relevantes:

1. Prompt Engineering (Engenharia de Prompt)
A Engenharia de Prompt é a prática de otimizar entradas ou perguntas (prompts) para obter as respostas mais eficazes de modelos de linguagem como o GPT. Ao criar prompts bem elaborados, é possível obter resultados mais relevantes, precisos e úteis de IA generativa.

Exemplo: Refinar um prompt para uma IA gerar resumos melhores ou mais detalhados de um texto.
2. Few-Shot Learning
Few-Shot Learning (Aprendizado com Poucos Exemplos) é uma técnica em que um modelo de IA é treinado para aprender tarefas com apenas alguns exemplos de dados. Ao contrário do aprendizado tradicional, que requer grandes quantidades de dados para treinamento, o few-shot learning visa generalizar a partir de um número reduzido de exemplos.

Exemplo: Um modelo que, ao ser alimentado com apenas alguns exemplos de tradução, é capaz de traduzir sentenças de um idioma para outro sem ter visto muitos pares de treinamento.
3. Zero-Shot Learning
O Zero-Shot Learning (Aprendizado sem Exemplos) é uma abordagem onde o modelo é capaz de realizar tarefas para as quais não foi explicitamente treinado, com base no entendimento e raciocínio geral. Ele consegue generalizar com base no conhecimento adquirido durante o treinamento sem precisar de dados específicos para cada tarefa.

Exemplo: Um modelo de IA que pode responder a perguntas sobre um tópico que nunca foi visto no treinamento, usando apenas o que já conhece sobre outros tópicos.
4. Generative AI (IA Generativa)
IA Generativa é um termo mais amplo para descrever modelos de IA que geram novos conteúdos (como texto, imagem, música, etc.) a partir de dados existentes. Esses modelos são capazes de criar coisas novas, como textos, imagens, vídeos e música, que se assemelham aos exemplos nos dados de treinamento.

Exemplo: Modelos como o GPT para texto e DALL·E para imagens são exemplos de IA generativa.
5. Self-Supervised Learning
Self-Supervised Learning (Aprendizado Auto-Supervisionado) é uma técnica de aprendizado de máquina onde o modelo cria suas próprias "etiquetas" a partir dos dados, ao invés de precisar de um rótulo externo fornecido por humanos. Isso permite que o modelo aprenda de maneira mais eficiente a partir de grandes volumes de dados não rotulados.

Exemplo: Modelos de linguagem como o GPT são treinados de forma auto-supervisionada, onde as palavras anteriores em uma frase ajudam a prever a próxima palavra.
6. Transfer Learning (Aprendizado por Transferência)
Transfer Learning é um método no qual um modelo treinado para uma tarefa é adaptado para outra tarefa. Isso é útil quando não há muitos dados disponíveis para a nova tarefa, pois o modelo já tem o conhecimento adquirido de uma tarefa similar.

Exemplo: Um modelo treinado para classificação de imagens pode ser adaptado para reconhecer outro tipo de objeto com menos dados.
7. Neural Architecture Search (NAS)
NAS é uma técnica para automatizar o processo de projeto de arquiteturas de redes neurais. Em vez de um engenheiro de IA criar manualmente a arquitetura de uma rede, o NAS utiliza algoritmos de otimização para explorar diferentes configurações e encontrar a mais eficiente.

Exemplo: O NAS pode ser usado para encontrar a melhor arquitetura de rede neural para um problema específico, como reconhecimento de imagem.
8. Explainable AI (XAI)
XAI (Inteligência Artificial Explicável) refere-se a sistemas de IA projetados para fornecer explicações compreensíveis para suas decisões e predições. A explicabilidade é importante para aumentar a transparência e confiança em modelos de IA, especialmente em áreas como saúde e finanças.

Exemplo: Um modelo de IA que diagnostica doenças deve ser capaz de explicar por que fez uma determinada recomendação de tratamento.
9. Multimodal AI
A IA Multimodal é a capacidade de integrar e processar diferentes tipos de dados simultaneamente, como texto, imagem, som e vídeo. Modelos multimodais podem entender e gerar conteúdo em vários formatos ao mesmo tempo.

Exemplo: Um modelo de IA que pode gerar uma descrição de uma imagem ou até mesmo gerar imagens a partir de uma descrição textual (como o DALL·E ou CLIP).
10. Synthetic Data
Dados Sintéticos são dados gerados artificialmente em vez de serem coletados do mundo real. São frequentemente usados para treinar modelos de IA em situações onde há falta de dados reais ou dados reais são difíceis de obter.

Exemplo: Gerar imagens de rostos para treinar um sistema de reconhecimento facial quando não é possível usar dados reais devido a questões de privacidade.
11. Federated Learning
O Federated Learning (Aprendizado Federado) é um método de aprendizado de máquina que permite treinar modelos sem mover os dados para um servidor central. Em vez disso, os modelos são treinados localmente em dispositivos (como smartphones) e as atualizações são compartilhadas de forma centralizada, mantendo a privacidade dos dados.

Exemplo: Treinamento de um modelo de previsão de texto diretamente em smartphones sem que os dados dos usuários sejam enviados para servidores.
12. Diffusion Models
Os Modelos de Difusão são uma classe de modelos generativos que geram dados de alta qualidade (como imagens e som) através de um processo iterativo de difusão reversa. Esses modelos têm se mostrado eficazes em tarefas como geração de imagens e são uma alternativa aos tradicionais Modelos GAN (Generative Adversarial Networks).

Exemplo: Modelos como o Stable Diffusion geram imagens de alta qualidade com base em prompts textuais.
Resumo:
Esses novos termos estão refletindo a diversidade e o avanço contínuo da IA. Eles abrangem áreas como geração de conteúdo, aprendizado de máquina, privacidade, e até arquitetura de redes neurais, permitindo o desenvolvimento de soluções mais inteligentes, eficientes e com maior capacidade de adaptação e explicação.

Se você quiser aprofundar mais em algum desses termos ou entender como eles se aplicam em contextos específicos, fique à vontade para perguntar!

# COMMAND ----------

!pip install openai

# COMMAND ----------

!pip install --upgrade typing_extensions

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install openai --upgrade
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import openai
import requests
import os

# Your Databricks token or OpenAI API key
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"

# Initialize OpenAI client
openai.api_key = DATABRICKS_TOKEN

# Define the endpoint for retrieving documents (simulate this with a custom API or database)
def retrieve_documents(query: str):
    # Example of an external API request (this could be to your own document store or an external service)
    search_url = "https://your-document-retrieval-service.com/search"
    params = {'query': query}
    response = requests.get(search_url, params=params)
    
    # Assuming the response contains relevant documents in JSON format
    documents = response.json()['documents']
    return documents

# Define a function to perform RAG (Retrieval-Augmented Generation)
def rag(query: str):
    # Retrieve relevant documents based on the user query
    retrieved_docs = retrieve_documents(query)

    # Combine the retrieved documents into one string (if needed, filter to top N results)
    context = "\n".join([doc['content'] for doc in retrieved_docs])

    # Send the query and the retrieved documents to the model for generation
    response = openai.Completion.create(
        engine="davinci-codex",  # You can use any available model like GPT-3, etc.
        prompt=f"Given the following context:\n\n{context}\n\nAnswer the question: {query}",
        max_tokens=150
    )
    
    return response.choices[0].text.strip()

# Example usage
query = "Tell me about the dataframe zhang"
answer = rag(query)
print(answer)


# COMMAND ----------

# MAGIC %md
# MAGIC # RAG EXAMPLE

# COMMAND ----------

import openai
import requests
import os

# Your Databricks token or OpenAI API key
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"

# Initialize OpenAI client
openai.api_key = DATABRICKS_TOKEN

# Define the endpoint for retrieving documents (simulate this with a custom API or database)
def retrieve_documents(query: str):
    # Example of an external API request (this could be to your own document store or an external service)
    search_url = "https://g1.globo.com/"
    params = {'query': query}
    response = requests.get(search_url, params=params)
    
    # Check if the response is valid JSON
    if response.status_code == 200:
        try:
            documents = response.json().get('documents', [])
        except ValueError:
            documents = []
    else:
        documents = []
    
    return documents

# Define a function to perform RAG (Retrieval-Augmented Generation)
def rag(query: str):
    # Retrieve relevant documents based on the user query
    retrieved_docs = retrieve_documents(query)

    # Combine the retrieved documents into one string (if needed, filter to top N results)
    context = "\n".join([doc['content'] for doc in retrieved_docs])

    # Send the query and the retrieved documents to the model for generation
    response = openai.Completion.create(
        engine="davinci-codex",  # You can use any available model like GPT-3, etc.
        prompt=f"Given the following context:\n\n{context}\n\nAnswer the question: {query}",
        max_tokens=150
    )
    
    return response.choices[0].text.strip()

# Example usage
query = "Tell me about the dataframe zhang"
answer = rag(query)
print(answer)

# COMMAND ----------

import openai
import requests
import os

# Your Databricks token or OpenAI API key
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"

# Initialize OpenAI client
openai.api_key = DATABRICKS_TOKEN

# Define the endpoint for retrieving documents (simulate this with a custom API or database)
def retrieve_documents(query: str):
    # Example of an external API request (this could be to your own document store or an external service)
    search_url = "https://g1.globo.com/"
    params = {'query': query}
    response = requests.get(search_url, params=params)
    
    # Assuming the response contains relevant documents in JSON format
    documents = response.json()['documents']
    return documents

# Define a function to perform RAG (Retrieval-Augmented Generation)
def rag(query: str):
    # Retrieve relevant documents based on the user query
    retrieved_docs = retrieve_documents(query)

    # Combine the retrieved documents into one string (if needed, filter to top N results)
    context = "\n".join([doc['content'] for doc in retrieved_docs])

    # Send the query and the retrieved documents to the model for generation
    response = openai.Completion.create(
        engine="davinci-codex",  # You can use any available model like GPT-3, etc.
        prompt=f"Given the following context:\n\n{context}\n\nAnswer the question: {query}",
        max_tokens=150
    )
    
    return response.choices[0].text.strip()

# Example usage
query = "Tell me about the dataframe zhang"
answer = rag(query)
print(answer)


# COMMAND ----------

#%python
# Install the latest version of typing_extensions
#%pip install --upgrade typing_extensions

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
#DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"
#DATABRICKS_TOKEN = '<TOKEN>'
# Alternatively in a Databricks notebook you can use this:
# DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://adb-6523536500265509.9.azuredatabricks.net/serving-endpoints"
)

chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are an AI assistant"
  },
  {
    "role": "user",
    "content": f"Tell me about this dataframne zhang"
  }
  ],
  #model="databricks-meta-llama-3-1-70b-instruct",
  model="databricks-meta-llama-3-1-405b-instruct",
  max_tokens=256
)

print(chat_completion.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rag example

# COMMAND ----------

from openai import OpenAI
import numpy as np
import os
from sklearn.metrics.pairwise import cosine_similarity

# Databricks token (ensure it is kept secure)
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"

# Initialize OpenAI client
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://adb-6523536500265509.9.azuredatabricks.net/serving-endpoints"
)

# Simulated dataframe (representing rows in your dataframe)
data = [
    {"ID": 1, "Name": "Zhang", "Age": 29, "Salary": 75000},
    {"ID": 2, "Name": "Li", "Age": 35, "Salary": 85000},
    {"ID": 3, "Name": "Wang", "Age": 42, "Salary": 95000},
    {"ID": 4, "Name": "Zhang", "Age": 22, "Salary": 70000},
    {"ID": 5, "Name": "Zhang", "Age": 31, "Salary": 72000}
]

# Simulate embedding function (in practice, you'd use OpenAI embeddings API or other models)
def generate_embedding(text):
    """Simulate generating an embedding for the given text."""
    # Normally, you'd use OpenAI's embeddings API, but here we'll use random embeddings for the sake of the example
    return np.random.rand(1, 512)  # Random vector of size 512

# Step 1: Generate embeddings for the dataframe rows
data_embeddings = []
for row in data:
    text = f"Name: {row['Name']}, Age: {row['Age']}, Salary: {row['Salary']}"
    embedding = generate_embedding(text)
    data_embeddings.append(embedding)

# Convert list of embeddings to a NumPy array
data_embeddings = np.vstack(data_embeddings)

# Step 2: Retrieve the most relevant rows based on the user's query
def retrieve_relevant_data(query, data_embeddings, k=3):
    """Retrieve the most relevant rows from the dataframe using cosine similarity."""
    # Generate the embedding for the user's query
    query_embedding = generate_embedding(query)
    
    # Compute cosine similarity between the query embedding and each row embedding
    similarities = cosine_similarity(query_embedding, data_embeddings)
    
    # Get the indices of the top k most similar rows
    top_k_indices = similarities.argsort()[0][-k:][::-1]
    
    # Retrieve the corresponding rows from the dataframe
    relevant_rows = [data[i] for i in top_k_indices]
    
    return relevant_rows

# Step 3: Retrieve the relevant data for the user's query
query = "Tell me about the Zhang dataframe"
relevant_data = retrieve_relevant_data(query, data_embeddings)

# Augment the user's query with the relevant data
augmented_query = f"{query}\n\nHere are the most relevant rows:\n"
for row in relevant_data:
    augmented_query += f"ID: {row['ID']}, Name: {row['Name']}, Age: {row['Age']}, Salary: {row['Salary']}\n"

# Step 4: Send the augmented query to the model for completion
chat_completion = client.chat.completions.create(
    messages=[
        {
            "role": "system",
            "content": "You are an AI assistant."
        },
        {
            "role": "user",
            "content": augmented_query
        }
    ],
    model="databricks-meta-llama-3-1-405b-instruct",  # Choose appropriate model
    max_tokens=256
)

# Output the response from the model
print(chat_completion.choices[0].message.content)


# COMMAND ----------

# Required libraries
from openai import OpenAI
import os

# Databricks token (ensure you are securing your token)
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"

# Initialize OpenAI client
client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://adb-6523536500265509.9.azuredatabricks.net/serving-endpoints"
)

# Simulating a "retriever" (RAG setup)
def retrieve_relevant_info(query):
    """
    Simulate the retrieval of relevant data from your data sources, such as databases or files.
    In a real use case, replace this with actual queries to your data store.
    """
    # Simulated relevant data about the "zhang" dataframe
    # In practice, replace this with actual retrieval logic (SQL queries, data searches, etc.)
    if "zhang" in query.lower():
        return "The 'zhang' dataframe contains the following columns: 'ID', 'Name', 'Age', 'Salary'. It was last updated on 2025-03-06."
    return "No relevant information found."

# Retrieve relevant data for the user’s question
query = "Tell me about this dataframe zhang"
retrieved_info = retrieve_relevant_info(query)

# Send the retrieved information with the model completion request
chat_completion = client.chat.completions.create(
  messages=[
    {
      "role": "system",
      "content": "You are an AI assistant."
    },
    {
      "role": "user",
      "content": f"Tell me about this dataframe zhang: {retrieved_info}"
    }
  ],
  model="databricks-meta-llama-3-1-405b-instruct",  # Choose appropriate model
  max_tokens=256
)

# Output the response from the model
print(chat_completion.choices[0].message.content)


# COMMAND ----------

from openai import OpenAI
import numpy as np
import os
from sklearn.metrics.pairwise import cosine_similarity

# Databricks token (ensure it is kept secure)
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"

# Initialize OpenAI client
client_openai = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://adb-6523536500265509.9.azuredatabricks.net/serving-endpoints"
)

# DeepSeek Client Placeholder
class DeepSeekClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://deepseek.example.com/api"  # Replace with actual DeepSeek endpoint

    def generate_response(self, prompt, model="deepseek-large"):
        # Simulate a request to DeepSeek API for response generation
        # Replace with actual logic to interact with the DeepSeek API
        response = {
            "choices": [
                {
                    "message": {"content": f"DeepSeek Response: {prompt}"}
                }
            ]
        }
        return response


# Initialize DeepSeek client
client_deepseek = DeepSeekClient(api_key=DATABRICKS_TOKEN)

# Simulated dataframe (representing rows in your dataframe)
data = [
    {"ID": 1, "Name": "Zhang", "Age": 29, "Salary": 75000},
    {"ID": 2, "Name": "Li", "Age": 35, "Salary": 85000},
    {"ID": 3, "Name": "Wang", "Age": 42, "Salary": 95000},
    {"ID": 4, "Name": "Zhang", "Age": 22, "Salary": 70000},
    {"ID": 5, "Name": "Zhang", "Age": 31, "Salary": 72000}
]

# Simulate embedding function (in practice, you'd use OpenAI embeddings API or other models)
def generate_embedding(text):
    """Simulate generating an embedding for the given text."""
    return np.random.rand(1, 512)  # Random vector of size 512

# Step 1: Generate embeddings for the dataframe rows
data_embeddings = []
for row in data:
    text = f"Name: {row['Name']}, Age: {row['Age']}, Salary: {row['Salary']}"
    embedding = generate_embedding(text)
    data_embeddings.append(embedding)

# Convert list of embeddings to a NumPy array
data_embeddings = np.vstack(data_embeddings)

# Step 2: Retrieve the most relevant rows based on the user's query
def retrieve_relevant_data(query, data_embeddings, k=3):
    """Retrieve the most relevant rows from the dataframe using cosine similarity."""
    query_embedding = generate_embedding(query)  # Generate embedding for query
    similarities = cosine_similarity(query_embedding, data_embeddings)
    top_k_indices = similarities.argsort()[0][-k:][::-1]
    
    # Retrieve the corresponding rows from the dataframe
    relevant_rows = [data[i] for i in top_k_indices]
    
    return relevant_rows

# Step 3: Retrieve relevant data based on query
query = "Tell me about the Zhang dataframe"
relevant_data = retrieve_relevant_data(query, data_embeddings)

# Augment the user's query with the relevant data
augmented_query = f"{query}\n\nHere are the most relevant rows:\n"
for row in relevant_data:
    augmented_query += f"ID: {row['ID']}, Name: {row['Name']}, Age: {row['Age']}, Salary: {row['Salary']}\n"

# Step 4: Select model and send the augmented query for completion
def generate_response_with_model(query, model="openai"):
    if model == "openai":
        # Use OpenAI API
        chat_completion = client_openai.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are an AI assistant."},
                {"role": "user", "content": query}
            ],
            model="databricks-meta-llama-3-1-405b-instruct",  # Choose appropriate OpenAI model
            max_tokens=256
        )
        return chat_completion.choices[0].message.content

    elif model == "deepseek":
        # Use DeepSeek API
        response = client_deepseek.generate_response(prompt=query)
        return response["choices"][0]["message"]["content"]

# Step 5: Decide on model selection (OpenAI or DeepSeek)
selected_model = "deepseek"  # Can be dynamic based on user preference or other logic

# Get the response from the selected model
response = generate_response_with_model(augmented_query, model=selected_model)

# Output the response
print(response)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import datediff, col, when, greatest
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, when

# Caminho datalake
var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# Importando base única
df = (spark.read
                  .option("delimiter", ";")
                  .option("header", "true")
                  .option("encoding", "ISO-8859-1")
                  .csv('{uri}/tmp/dev/uld/oni/mte/rais/identificada/rais_vinculo/RAIS_2023/RAIS_VINC_ID_SP/'.format(uri=var_adls_uri))
                  )

# COMMAND ----------

#result_df = spark.sql("""SELECT * FROM `datalake__raw_usr`.`oni`.`oni_mte_rais_identificada__rais_vinculo`;""")

result_df = spark.sql("""SELECT * FROM datalake__raw_usr.oni.oni_mte_rais_identificada__rais_vinculo""")

# COMMAND ----------

# dapic317bb822bd598dad33644441c6beb57

# Install the latest version of typing_extensions
#####%pip install --upgrade typing_extensions

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
#DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
DATABRICKS_TOKEN = "dapic317bb822bd598dad33644441c6beb57"
#DATABRICKS_TOKEN = '<TOKEN>'
# Alternatively in a Databricks notebook you can use this:
# DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://adb-6523536500265509.9.azuredatabricks.net/serving-endpoints"
)

chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are an AI assistant"
  },
  {
    "role": "user",
    "content": f"Quais anos possuem os dados da RAIS nesse linke de ftp ftp://ftp.mtps.gov.br/pdet/microdados/, acessar ele e me mostra tudo"
  }
  ],
  #model="databricks-meta-llama-3-1-70b-instruct",
  model="databricks-meta-llama-3-1-405b-instruct",
  max_tokens=256
)

print(chat_completion.choices[0].message.content)

# COMMAND ----------

# Install the latest version of typing_extensions
#####%pip install --upgrade typing_extensions

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
#DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
DATABRICKS_TOKEN = "dapi08d6c7e4312b742e3aa8c778128daf18"
#DATABRICKS_TOKEN = '<TOKEN>'
# Alternatively in a Databricks notebook you can use this:
# DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://adb-6523536500265509.9.azuredatabricks.net/serving-endpoints"
)

chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are an AI assistant"
  },
  {
    "role": "user",
    "content": f"Quais anos possuem os dados da RAIS nesse linke de ftp ftp://ftp.mtps.gov.br/pdet/microdados/, acessar ele e me mostra tudo"
  }
  ],
  #model="databricks-meta-llama-3-1-70b-instruct",
  model="databricks-meta-llama-3-1-405b-instruct",
  max_tokens=256
)

print(chat_completion.choices[0].message.content)

# COMMAND ----------

Segue um exemplo de código em Python utilizando a biblioteca PySpark para validação de dados em cada coluna do DataFrame:
```python
from pyspark.sql.functions import col, when

# Crie um objeto DataFrame
df = spark.createDataFrame([...])  # substitua [...] pelo seu DataFrame

# Defina as regras de validação para cada coluna
validations = {
    "CD_MUNICIPIO": lambda x: x.isNotNull() & (x >= 1) & (x <= 9999),
    "CD_CNAE10_CLASSE": lambda x: x.isNotNull() & (x >= 1) & (x <= 99),
    "FL_VINCULO_ATIVO_3112": lambda x: x.isNotNull() & (x == 1 | x == 0),
    # adicione mais regras para as outras colunas...
}

# Aplique as regras de validação para cada coluna
for col_name, validation in validations.items():
    df = df.withColumn(f"valid_{col_name}", validation(col(col_name)))

# Verifique se há erros de validação
errors = df.select([col(f"valid_{col_name}")

# COMMAND ----------

import pyspark.sql.functions as F

# Verificar se há valores nulos em cada coluna
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Verificar a porcentagem de valores nulos em cada coluna
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) / df.count() * 100 for c in df.columns]).show()

# Verificar a distribuição de valores em cada coluna
df.describe().show()

# Verificar a validade dos formatos de datas
df.select('DT_DIA_MES_ANO_DATA_ADMISSAO', 'DT_DIA_MES_ANO_DATA_NASCIMENTO', 'DT_DIA_MES_ANO_DIA_DESLIGAMENTO').apply(lambda x: F.when(F.to_date(x).isNotNull(), 'Válido').otherwise('Inválido')).show()

# Verificar a consistência dos valores em cada coluna
df.groupBy('CD_MUNICIPIO').agg(F.count('*')).filter(F.col('count') > 1)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
#DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
DATABRICKS_TOKEN = "dapie4d51ec90b07394e237aa8501de7e219"
#DATABRICKS_TOKEN = '<TOKEN>'
# Alternatively in a Databricks notebook you can use this:
# DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://adb-6523536500265509.9.azuredatabricks.net/serving-endpoints"
)

chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are an AI assistant"
  },
  {
    "role": "user",
    "content": f"Tell me about this dataframne zhang"
  }
  ],
  #model="databricks-meta-llama-3-1-70b-instruct",
  model="databricks-meta-llama-3-1-405b-instruct",
  max_tokens=256
)

print(chat_completion.choices[0].message.content)

# PermissionDeniedError: Error code: 403 - {'error_code': 'PERMISSION_DENIED', 'message': 'PERMISSION_DENIED: The endpoint is disabled due to a rate limit set to 0.'}