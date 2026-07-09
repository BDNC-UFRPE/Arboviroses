if (!require("tidyverse")) install.packages("tidyverse")
if (!require("mongolite")) install.packages("mongolite")

library(tidyverse)
library(mongolite)

# 1. CONEXÃO COM AS COLEÇÕES DO MONGODB (BANCO ARBOVIROSES1)
db_prata <- mongo(collection = "notificacoes", db = "arboviroses1", url = "mongodb://localhost:27017")

db_ouro_1 <- mongo(collection = "ouro_gravidade_comorbidades", db = "arboviroses1", url = "mongodb://localhost:27017")
db_ouro_2 <- mongo(collection = "ouro_atraso_clinico", db = "arboviroses1", url = "mongodb://localhost:27017")
db_ouro_3 <- mongo(collection = "ouro_demografia_letalidade", db = "arboviroses1", url = "mongodb://localhost:27017")
db_ouro_4 <- mongo(collection = "ouro_comorbidades_detalhado", db = "arboviroses1", url = "mongodb://localhost:27017")

# Limpa as coleções Ouro antigas antes de reiniciar a carga
db_ouro_1$drop(); db_ouro_2$drop(); db_ouro_3$drop(); db_ouro_4$drop()

# Definimos o horizonte temporal total do projeto
anos_totais <- 2016:2026

cat("Iniciando o processamento incremental da Camada Ouro...\n")

for (ano_atual in anos_totais) {
  cat("\n--------------------------------------------------\n")
  cat("Agregando dados do Ano:", ano_atual, "\n")
  cat("--------------------------------------------------\n")
  
  # Filtro base em formato string JSON para o estágio inicial de cada agregação
  match_ano <- paste0('{"$match": {"ano": ', ano_atual, '}}')
  
  # ==============================================================================
  # COLECÃO OURO 1: ouro_gravidade_comorbidades (Efeito Dose-Resposta)
  # ==============================================================================
  cat("-> Computando Ouro 1 (Dose-Resposta)...\n")
  pipeline_ouro1 <- paste0('[',
                           match_ano, ',',
                           '{"$group": {
      "_id": {
        "doenca": "$doenca",
        "ano": "$ano",
        "n_comorbidades": "$n_comorbidades"
      },
      "total_casos": {"$sum": 1},
      "total_hospitalizados": {"$sum": {"$cond": [{"$eq": ["$hospitalizado", true]}, 1, 0]}},
      "total_graves": {"$sum": {"$cond": [{"$eq": ["$gravidade", "grave"]}, 1, 0]}},
      "total_obitos": {"$sum": {"$cond": [{"$eq": ["$obito", true]}, 1, 0]}}
    }},
    {"$project": {
      "_id": 0,
      "doenca": "$_id.doenca",
      "ano": "$_id.ano",
      "n_comorbidades": "$_id.n_comorbidades",
      "total_casos": 1,
      "total_hospitalizados": 1,
      "total_graves": 1,
      "total_obitos": 1
    }}
  ]')
  res_ouro1 <- db_prata$aggregate(pipeline_ouro1)
  if(nrow(res_ouro1) > 0) db_ouro_1$insert(res_ouro1)
  
  # ==============================================================================
  # COLECÃO OURO 2: ouro_atraso_clinico (Gargalos e Linha do Tempo)
  # ==============================================================================
  cat("-> Computando Ouro 2 (Atrasos Clínicos)...\n")
  pipeline_ouro2 <- paste0('[',
                           match_ano, ',',
                           '{"$match": {"atraso_dias": {"$ne": null}}},', # Filtra apenas quem tem cálculo válido
                           '{"$group": {
      "_id": {
        "doenca": "$doenca",
        "ano": "$ano",
        "uf": "$uf",
        "atraso_proxy": "$atraso_proxy"
      },
      "media_atraso": {"$avg": "$atraso_dias"},
      "total_casos_com_atraso": {"$sum": 1}
    }},
    {"$project": {
      "_id": 0,
      "doenca": "$_id.doenca",
      "ano": "$_id.ano",
      "uf": "$_id.uf",
      "atraso_proxy": "$_id.atraso_proxy",
      "media_atraso": 1,
      "total_casos_com_atraso": 1
    }}
  ]')
  res_ouro2 <- db_prata$aggregate(pipeline_ouro2)
  if(nrow(res_ouro2) > 0) db_ouro_2$insert(res_ouro2)
  
  # ==============================================================================
  # COLECÃO OURO 3: ouro_demografia_letalidade (Perfil Global)
  # ==============================================================================
  cat("-> Computando Ouro 3 (Demografia e Letalidade)...\n")
  pipeline_ouro3 <- paste0('[',
                           match_ano, ',',
                           '{"$group": {
      "_id": {
        "doenca": "$doenca",
        "ano": "$ano",
        "uf": "$uf",
        "sexo": "$sexo",
        "faixa_etaria": "$faixa_etaria"
      },
      "total_casos": {"$sum": 1},
      "total_obitos": {"$sum": {"$cond": [{"$eq": ["$obito", true]}, 1, 0]}}
    }},
    {"$project": {
      "_id": 0,
      "doenca": "$_id.doenca",
      "ano": "$_id.ano",
      "uf": "$_id.uf",
      "sexo": "$_id.sexo",
      "faixa_etaria": "$_id.faixa_etaria",
      "total_casos": 1,
      "total_obitos": 1
    }}
  ]')
  res_ouro3 <- db_prata$aggregate(pipeline_ouro3)
  if(nrow(res_ouro3) > 0) db_ouro_3$insert(res_ouro3)
  
  # ==============================================================================
  # COLECÃO OURO 4: ouro_comorbidades_detalhado (Assinatura de Severidade - Heatmap Q1)
  # ==============================================================================
  cat("-> Computando Ouro 4 (Assinatura por Patologia Crônica)...\n")
  
  # Esta query usa o $unwind para abrir o array de strings e computar cada comorbidade isolada
  pipeline_ouro4 <- paste0('[',
                           match_ano, ',',
                           '{"$match": {"comorbidades.lista": {"$exists": true, "$not": {"$size": 0}}}},', # Apenas quem tem comorbidades
                           '{"$unwind": "$comorbidades.lista"},', # Desmembra o array NoSQL em linhas planas
                           '{"$group": {
      "_id": {
        "doenca": "$doenca",
        "ano": "$ano",
        "uf": "$uf",
        "sexo": "$sexo",
        "faixa_etaria": "$faixa_etaria",
        "comorbidade": "$comorbidades.lista"
      },
      "total_casos": {"$sum": 1},
      "total_hospitalizados": {"$sum": {"$cond": [{"$eq": ["$hospitalizado", true]}, 1, 0]}},
      "total_graves": {"$sum": {"$cond": [{"$eq": ["$gravidade", "grave"]}, 1, 0]}},
      "total_obitos": {"$sum": {"$cond": [{"$eq": ["$obito", true]}, 1, 0]}}
    }},
    {"$project": {
      "_id": 0,
      "doenca": "$_id.doenca",
      "ano": "$_id.ano",
      "uf": "$_id.uf",
      "sexo": "$_id.sexo",
      "faixa_etaria": "$_id.faixa_etaria",
      "comorbidade": "$_id.comorbidade",
      "total_casos": 1,
      "total_hospitalizados": 1,
      "total_graves": 1,
      "total_obitos": 1
    }}
  ]')
  res_ouro4 <- db_prata$aggregate(pipeline_ouro4)
  if(nrow(res_ouro4) > 0) db_ouro_4$insert(res_ouro4)
  
  # Força a limpeza de memória RAM do R após fechar cada ano
  gc()
}

cat("\n==================================================\n")
cat("Pipeline concluído! Volumetria na Camada Ouro:\n")
cat("Ouro 1 (Dose-Resposta):", db_ouro_1$count(), "registros.\n")
cat("Ouro 2 (Atraso Clínico):", db_ouro_2$count(), "registros.\n")
cat("Ouro 3 (Demografia):     ", db_ouro_3$count(), "registros.\n")
cat("Ouro 4 (Detalhamento):   ", db_ouro_4$count(), "registros.\n")
cat("==================================================\n")