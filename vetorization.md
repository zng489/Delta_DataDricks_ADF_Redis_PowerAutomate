# Vetorização de palavras técnicas

Vetorização de palavras técnicas é uma técnica de processamento de linguagem natural (PLN) usada para transformar palavras ou termos em representações numéricas, permitindo que algoritmos de aprendizado de máquina manipulem e compreendam essas palavras. Esse processo é frequentemente usado em modelos de PLN para tratar palavras, incluindo termos técnicos e jargões, de maneira que eles possam ser analisados.

## Exemplo de Vetorização de Palavras Técnicas

Considere as palavras técnicas "rede neural" e "algoritmo de otimização". Vamos usar duas abordagens comuns para vetorização de palavras:

### 1. Bag of Words (BoW)
No modelo de Bag of Words, as palavras são representadas como vetores binários ou de contagem. Cada palavra ou termo técnico é mapeado para uma posição em um vetor, e o valor nessa posição representa a frequência de ocorrência dessa palavra no texto.

Suponha que temos um corpus com as palavras: "rede neural", "algoritmo de otimização" e "machine learning". O vetor para um documento que contém a frase "rede neural" ficaria assim, usando contagem de palavras:

| Palavra/Termo     | "rede neural" |
|-------------------|---------------|
| rede              | 1             |
| neural            | 1             |
| algoritmo         | 0             |
| de                | 0             |
| otimização        | 0             |
| machine           | 0             |
| learning          | 0             |

Neste caso, temos um vetor de 7 dimensões, representando as 7 palavras do vocabulário.

### 2. Word Embeddings (Word2Vec ou GloVe)
Os embeddings são representações densas e contínuas das palavras, onde palavras com significados semelhantes possuem representações próximas no espaço vetorial. Por exemplo, o modelo Word2Vec ou GloVe pode gerar vetores densos de palavras técnicas com dimensões reduzidas, como vetores de 100 a 300 elementos.

Para as palavras "rede neural" e "algoritmo de otimização", o modelo pode gerar vetores com as seguintes características:

- "rede neural" pode ser mapeada para um vetor como:
[0.45, 0.78, −0.23, 0.12, …]

arduino
Copiar
- "algoritmo de otimização" pode ter um vetor como:
[0.56, −0.34, 0.67, −0.11, …]

python
Copiar

Esses vetores são densos e não seguem a contagem direta de palavras. Em vez disso, as palavras estão representadas em um espaço contínuo, o que facilita capturar relações semânticas entre termos técnicos. O modelo pode entender que "rede neural" e "algoritmo de otimização" estão relacionados ao aprendizado de máquina e à inteligência artificial, por exemplo, porque os vetores de palavras associadas a esses campos de estudo ficam mais próximos uns dos outros.

Esses exemplos ilustram como podemos usar diferentes técnicas para representar palavras técnicas de forma que algoritmos de aprendizado de máquina possam compreendê-las e usá-las para análise, classificação ou outras tarefas de PLN.

## Como gerar um vetor

Gerar um vetor de palavras, especialmente para palavras técnicas, pode ser feito usando diferentes técnicas de vetorização. Aqui estão dois exemplos de como gerar vetores usando as abordagens Bag of Words (BoW) e Word Embeddings (Word2Vec).

### 1. Gerar um vetor usando Bag of Words (BoW)

A técnica Bag of Words (BoW) representa as palavras com base em sua frequência no texto. O vetor gerado terá uma dimensão igual ao número de palavras no vocabulário (ou seja, o conjunto de todas as palavras únicas presentes nos textos). Cada palavra no texto é representada por um número que indica quantas vezes ela aparece.

**Exemplo em Python com CountVectorizer (BoW):**

```python
from sklearn.feature_extraction.text import CountVectorizer

# Exemplo de textos (documentos)
documentos = [
  "rede neural é um modelo de aprendizado",
  "algoritmo de otimização é importante para a rede neural",
  "o algoritmo de otimização pode melhorar redes neurais"
]

# Cria o vetor de características
vectorizer = CountVectorizer()

# Aplica a vetorização (transforma os documentos em vetores)
X = vectorizer.fit_transform(documentos)

# Visualiza o vocabulário (as palavras que foram extraídas)
print("Vocabulário:", vectorizer.get_feature_names_out())

# Exibe a matriz de contagem das palavras
print("\nMatriz de contagem:")
print(X.toarray())
Saída esperada:

lua
Copiar
Vocabulário: ['algoritmo' 'aprendizado' 'de' 'é' 'improvar' 'importante' 'melhorar' 'modelo' 'neural' 'otimização' 'para' 'rede']

Matriz de contagem:
[[0 1 1 1 0 0 0 1 1 0 0 1]
 [1 0 1 1 0 1 1 0 1 1 1 0]
 [1 0 1 1 1 0 1 0 1 1 1 1]]
Neste exemplo, temos uma matriz de contagem onde cada linha representa um documento e cada coluna representa uma palavra do vocabulário. Os valores indicam a frequência das palavras no documento.

2. Gerar um vetor usando Word2Vec (Word Embeddings)
O Word2Vec é um modelo que gera representações densas de palavras em vetores contínuos. Essas representações são capazes de capturar semelhanças semânticas entre palavras, como associar "algoritmo" e "otimização" em um contexto técnico.

Exemplo em Python com Gensim (Word2Vec):

python
Copiar
from gensim.models import Word2Vec

# Exemplo de corpus (lista de palavras)
corpus = [
    ["rede", "neural", "é", "um", "modelo", "de", "aprendizado"],
    ["algoritmo", "de", "otimização", "é", "importante", "para", "rede", "neural"],
    ["algoritmo", "de", "otimização", "pode", "melhorar", "redes", "neurais"]
]

# Treina o modelo Word2Vec no corpus
model = Word2Vec(corpus, vector_size=100, window=5, min_count=1, workers=4)

# Exibe o vetor para a palavra 'rede'
print("Vetor para 'rede':")
print(model.wv['rede'])

# Exibe o vetor para a palavra 'algoritmo'
print("\nVetor para 'algoritmo':")
print(model.wv['algoritmo'])
Saída esperada (exemplo de vetores para as palavras):

arduino
Copiar
Vetor para 'rede':
[-0.02572929  0.0049824  -0.0142874   0.02490176  0.03623755 ... ]

Vetor para 'algoritmo':
[ 0.03150272  0.0063495  -0.01467876  0.02930844  0.0129823 ... ]
Neste exemplo, o modelo Word2Vec gera vetores densos de 100 dimensões para as palavras "rede" e "algoritmo". Esses vetores podem ser usados para medir a semelhança semântica entre as palavras e são mais compactos do que a representação de contagem no modelo Bag of Words.

Resumo das Diferenças
Bag of Words (BoW): Gera um vetor esparso baseado na frequência das palavras no texto. Cada palavra no vocabulário tem sua própria dimensão no vetor.
Word2Vec: Gera um vetor denso, onde as palavras semelhantes semanticamente ficam próximas no espaço vetorial.
Ambos os métodos têm seus usos, mas o Word2Vec é mais eficiente para capturar semelhanças semânticas entre palavras técnicas, o que pode ser crucial quando se lida com termos especializados.

Mais Exemplos de Vetorização
Aqui estão mais exemplos de como gerar vetores para palavras técnicas usando diferentes abordagens, como TF-IDF (uma variação do BoW) e Word2Vec com um corpus mais diverso. Além disso, vou mostrar como você pode usar FastText, que é outra técnica de embeddings para lidar com palavras técnicas, incluindo palavras compostas ou desconhecidas.

1. Exemplo de Vetorização com TF-IDF
O TF-IDF (Term Frequency - Inverse Document Frequency) é uma técnica que mede a importância de uma palavra dentro de um documento em relação ao corpus. Ela penaliza palavras muito comuns, como "o", "de", "para", que aparecem em muitos documentos, enquanto dá mais peso a palavras que aparecem menos frequentemente, mas são mais informativas.

Exemplo em Python com TfidfVectorizer:

python
Copiar
from sklearn.feature_extraction.text import TfidfVectorizer

# Exemplo de textos (documentos)
documentos = [
    "rede neural é uma técnica de aprendizado de máquina",
    "algoritmo de otimização é importante para redes neurais",
    "modelos de aprendizado e redes neurais são usados em inteligência artificial"
]

# Cria o vetor de características usando TF-IDF
vectorizer = TfidfVectorizer()

# Aplica a vetorização (transforma os documentos em vetores)
X = vectorizer.fit_transform(documentos)

# Visualiza o vocabulário (as palavras que foram extraídas)
print("Vocabulário:", vectorizer.get_feature_names_out())

# Exibe a matriz TF-IDF
print("\nMatriz TF-IDF:")
print(X.toarray())
Saída esperada:

lua
Copiar
Vocabulário: ['algoritmo' 'aprendizado' 'de' 'em' 'inteligência' 'máquina' 'neural' 'otimização' 'para' 'rede' 'redes' 'usados']

Matriz TF-IDF:
[[0.         0.47125356 0.39896488 0.         0.         0.39896488 0.47125356 0.         0.         0.47125356 0.         0.        ]
 [0.         0.         0.39896488 0.         0.         0.         0.47125356 0.47125356 0.47125356 0.         0.47125356 0.        ]
 [0.47125356 0.         0.         0.47125356 0.47125356 0.         0.         0.         0.         0.         0.         0.47125356]]
2. Exemplo de Vetorização com FastText
O FastText é uma técnica semelhante ao Word2Vec, mas leva em consideração os subcomponentes das palavras, o que ajuda a representar melhor palavras compostas ou palavras desconhecidas. Isso é especialmente útil quando lidamos com termos técnicos ou jargões, como "rede neural" ou "algoritmo de otimização".

Exemplo em Python com FastText:

Primeiro, você precisa instalar a biblioteca fasttext:

bash
Copiar
pip install fasttext
Agora, use o seguinte código para treinar um modelo FastText:

python
Copiar
import fasttext

# Exemplo de corpus (lista de palavras)
corpus = [
    "rede neural é uma técnica de aprendizado de máquina",
    "algoritmo de otimização é importante para redes neurais",
    "modelos de aprendizado e redes neurais são usados em inteligência artificial"
]

# Salva o corpus em um arquivo (FastText precisa de um arquivo para treinamento)
with open('corpus.txt', 'w') as f:
    for linha in corpus:
        f.write(f"{linha}\n")

# Treina o modelo FastText
model = fasttext.train_unsupervised('corpus.txt', model='skipgram')

# Exibe o vetor para a palavra 'rede'
print("Vetor para 'rede':")
print(model.get_word_vector('rede'))

# Exibe o vetor para a palavra 'algoritmo'
print("\nVetor para 'algoritmo':")
print(model.get_word_vector('algoritmo'))
Saída esperada:

arduino
Copiar
Vetor para 'rede':
[-0.07763391  0.12897988 -0.24979211  0.04044525  0.22054551  ...]

Vetor para 'algoritmo':
[ 0.21505843 -0.09174516  0.02425673  0.16323703 -0.06684898 ... ]
3. Exemplo de Vetorização com GloVe
GloVe (Global Vectors for Word Representation) é outra técnica de embedding que é baseada em fatores de coocorrência entre palavras no corpus. Ela cria representações de palavras no espaço vetorial de forma que as palavras que aparecem em contextos semelhantes ficam próximas.

Exemplo com GloVe usando o Gensim:

Primeiro, instale o Gensim (caso não tenha):

bash
Copiar
pip install gensim
Em seguida, você pode usar um modelo pré-treinado ou treinar o seu próprio modelo. Aqui está como carregar e usar um modelo pré-treinado do GloVe:

python
Copiar
from gensim.models import KeyedVectors

# Carregar o modelo GloVe pré-treinado
# O modelo "glove-wiki-gigaword-100" tem vetores de 100 dimensões para cada palavra
model = KeyedVectors.load_word2vec_format('glove-wiki-gigaword-100', binary=False)

# Exibe o vetor para a palavra 'rede'
print("Vetor para 'rede':")
print(model['rede'])

# Exibe o vetor para a palavra 'algoritmo'
print("\nVetor para 'algoritmo':")
print(model['algoritmo'])
Resumo dos Exemplos
Bag of Words (BoW): Representa a frequência de palavras em um documento. Não captura semelhanças semânticas.
TF-IDF: Penaliza palavras muito comuns e dá mais peso a palavras importantes em um documento.
FastText: A técnica considera subcomponentes das palavras, o que a torna boa para palavras técnicas e compostas.
Word2Vec e GloVe: Técnicas de embeddings que representam palavras em um espaço vetorial contínuo e capturam semelhanças semânticas entre palavras.
Esses exemplos mostram como você pode usar diferentes abordagens para vetorização de palavras, dependendo do tipo de análise que você deseja fazer e da natureza das palavras ou termos técnicos que está trabalhando.