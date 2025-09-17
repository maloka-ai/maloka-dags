# Otimização do Processamento Paralelo para FP-Growth

## ATUALIZAÇÃO: Correção de Erros (10/09/2025)

Foi implementada uma correção para resolver o erro: `cannot access local variable 'n_cpus' where it is not associated with a value`. 

### Principais correções:
1. Movida a definição da variável `n_cpus` para fora do bloco condicional, tornando-a acessível em todo o escopo da função
2. Melhorado o tratamento de exceções para capturar erros específicos e genéricos
3. Implementado um sistema de fallback em múltiplos níveis que tenta diferentes abordagens caso o processamento paralelo falhe
4. Adicionada proteção adicional contra erros no método tradicional sem paralelização

## Melhorias Implementadas

A otimização para o algoritmo FP-Growth foi aprimorada para lidar melhor com conjuntos de dados muito grandes. As principais melhorias são:

### 1. Ajuste Dinâmico de Suporte para Chunks

- **Problema anterior**: O suporte mínimo era aplicado igualmente a todos os chunks, o que poderia resultar em falsos negativos para itemsets que estariam acima do limite quando considerados no conjunto completo.
- **Solução**: Implementamos um ajuste dinâmico do suporte mínimo para cada chunk, levando em consideração seu tamanho proporcional ao dataset completo.

### 2. Gerenciamento de Memória Adaptativo

- **Problema anterior**: A divisão dos chunks era baseada apenas no número de CPUs, o que poderia causar problemas de memória com datasets muito grandes.
- **Solução**: Adicionamos um cálculo para estimar o consumo de memória e dividir o dataset considerando tanto os recursos de CPU quanto a memória disponível.

### 3. Tratamento de Erros de Memória

- Implementamos um mecanismo para detectar erros de memória e reduzir dinamicamente o número de processos quando isso ocorre.

### 4. Combinação Eficiente de Resultados

- **Problema anterior**: A concatenação simples e eliminação de duplicatas não tratava corretamente os resultados paralelos.
- **Solução**: Adicionamos uma etapa para agrupar os resultados por itemsets idênticos e calcular a média ponderada dos suportes.

## Como Funciona

1. **Divisão Inteligente dos Dados**: O dataset é dividido considerando tanto o número de CPUs quanto o tamanho estimado em memória.
2. **Ajuste de Suporte por Chunk**: O suporte mínimo é ajustado proporcionalmente para cada chunk.
3. **Processamento Paralelo com Segurança**: Cada chunk é processado separadamente com limites de memória.
4. **Combinação de Resultados Ponderada**: Os resultados são agrupados e os suportes são calculados corretamente.

## Parâmetros Adicionais

- `memory_limit_gb`: Permite definir o limite de memória por worker (padrão: 4GB).
- Detecção automática do número ideal de chunks baseado no tamanho do dataset.

## Uso Recomendado

Para datasets muito grandes, considere ajustar o parâmetro `memory_limit_gb` de acordo com a disponibilidade de RAM do seu sistema.

```python
# Exemplo de uso
frequent_itemsets = parallel_fpgrowth(
    df_grande, 
    min_support=0.01, 
    use_colnames=True,
    memory_limit_gb=2  # Ajuste de acordo com seu sistema
)
```

Esta implementação garante melhor utilização dos recursos do sistema e maior precisão nos resultados para conjuntos de dados de qualquer tamanho.
