if (!require("mongolite")) install.packages("mongolite")
library(mongolite)

# 1. CONEXÃO COM A BASE PRATA
cat("Conectando à base Prata...\n")
db_prata <- mongo(collection = "notificacoes", db = "arboviroses", url = "mongodb://localhost:27017")

# ==============================================================================
# PARTE A: LIMPEZA DE INCONSISTÊNCIAS DE DATA (REMOÇÃO DE REGISTROS OUTLIERS)
# ==============================================================================
total_antes <- db_prata$count()
erros_altos <- db_prata$count('{"atraso_dias": {"$gt": 30}}')
erros_negativos <- db_prata$count('{"atraso_dias": {"$lt": 0}}')

cat("\n--- Cenário de Outliers no Banco ---\n")
cat("Total de registros na Prata:", total_antes, "\n")
cat("Casos com atraso > 30 dias (erros):", erros_altos, "\n")
cat("Casos com atraso negativo (datas invertidas):", erros_negativos, "\n")

if (erros_altos > 0 || erros_negativos > 0) {
  cat("\nIniciando a remoção dos dados inconsistentes (atraso_dias)...\n")
  
  # Remove maiores que 30
  db_prata$remove('{"atraso_dias": {"$gt": 30}}')
  
  # Remove menores que 0
  db_prata$remove('{"atraso_dias": {"$lt": 0}}')
  
  total_depois_datas <- db_prata$count()
  cat("Total de registros deletados por erro de data:", (total_antes - total_depois_datas), "\n")
} else {
  cat("\nNenhum registro com erro de atraso_dias precisou ser removido.\n")
}

# ==============================================================================
# PARTE B: PURIFICAÇÃO NOSQL (REMOÇÃO DE CAMPOS INEXISTENTES EM ZIKA E FEBRE AMARELA)
# ==============================================================================
cat("\n--- Iniciando a Purificação NoSQL para Zika e Febre Amarela ---\n")

# Filtros para identificar se ainda existem documentos com essas propriedades
campos_zika <- db_prata$count('{"doenca": "zika", "gravidade": {"$exists": true}}')
campos_fa   <- db_prata$count('{"doenca": "febre_amarela", "gravidade": {"$exists": true}}')

if (campos_zika > 0 || campos_fa > 0) {
  cat("Removendo chaves redundantes (gravidade, hospitalizado, n_comorbidades, comorbidades)...\n")
  
  # O operador $unset remove as chaves do JSON estruturado
  query_unset <- '{"$unset": {"hospitalizado": "", "gravidade": "", "n_comorbidades": "", "comorbidades": ""}}'
  
  # Aplica no Zika
  db_prata$update(
    query = '{"doenca": "zika"}',
    update = query_unset,
    multiple = TRUE
  )
  cat("-> Campos limpos com sucesso nos registros de Zika!\n")
  
  # Aplica na Febre Amarela
  db_prata$update(
    query = '{"doenca": "febre_amarela"}',
    update = query_unset,
    multiple = TRUE
  )
  cat("-> Campos limpos com sucesso nos registros de Febre Amarela!\n")
  
} else {
  cat("A base já está purificada! Zika e Febre Amarela não possuem essas chaves.\n")
}

total_final <- db_prata$count()
cat("\n--- Processo Geral de Saneamento Concluído! ---\n")
cat("Total de registros ativos finais na Prata:", total_final, "\n\n")