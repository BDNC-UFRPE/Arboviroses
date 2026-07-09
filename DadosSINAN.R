if (!require("tidyverse")) install.packages("tidyverse")
if (!require("mongolite")) install.packages("mongolite")
if (!require("jsonlite")) install.packages("jsonlite")

library(tidyverse)
library(mongolite)
library(jsonlite)

options(HTTPUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# 1. CONSTRUÇÃO DO DICIONÁRIO DE UFs VIA API DO IBGE
cat("Buscando tabela oficial de UFs diretamente da API do IBGE...\n")
uf_api <- jsonlite::fromJSON("https://servicodados.ibge.gov.br/api/v1/localidades/estados")
mapa_uf_ibge <- setNames(uf_api$sigla, as.character(uf_api$id))

# 2. CONEXÃO COM O MONGODB (ALTERADO PARA ARBOVIROSES1)
db <- mongo(collection = "notificacoes", db = "arboviroses1", url = "mongodb://localhost:27017")

# ==============================================================================
# LOTE 1: DENGUE E CHIKUNGUNYA (Leitura em Chunks para não travar o PC)
# ==============================================================================
doencas_comorbidades <- c("Dengue", "Chikungunya")

processar_chunk_dengue_chik <- function(chunk, pos, doenca, ano, mapa_uf_ibge, db) {
  colunas_obrigatorias <- c("SG_UF_NOT", "NU_ANO", "NU_IDADE_N", "CS_SEXO", "DT_SIN_PRI", 
                            "DT_INTERNA", "EVOLUCAO", "CLASSI_FIN", "DIABETES", 
                            "HIPERTENSA", "RENAL", "HEPATOPAT", "HEMATOLOG", "AUTO_IMUNE", "ACIDO_PEPT")
  
  missing_cols <- setdiff(colunas_obrigatorias, names(chunk))
  if(length(missing_cols) > 0) {
    chunk[missing_cols] <- NA_character_
  }
  
  df_clean <- chunk %>%
    filter(as.character(NU_ANO) == as.character(ano)) %>%
    select(uf = SG_UF_NOT, ano = NU_ANO, NU_IDADE_N, CS_SEXO, DT_SIN_PRI, DT_INTERNA, EVOLUCAO, CLASSI_FIN,
           DIABETES, HIPERTENSA, RENAL, HEPATOPAT, HEMATOLOG, AUTO_IMUNE, ACIDO_PEPT) %>%
    rowwise() %>%
    mutate(
      uf = coalesce(unname(mapa_uf_ibge[as.character(uf)]), as.character(uf)),
      nu_idade_str = stringr::str_pad(as.character(NU_IDADE_N), 4, pad = "0"),
      id_tipo = substr(nu_idade_str, 1, 1),
      id_valor = as.numeric(substr(nu_idade_str, 2, 4)),
      idade = case_when(id_tipo == "4" ~ id_valor, id_tipo == "3" ~ id_valor / 12, id_tipo %in% c("1", "2") ~ 0, TRUE ~ NA_real_),
      faixa_etaria = case_when(idade < 1 ~ "<1", idade <= 4 ~ "1-4", idade <= 11 ~ "5-11", idade <= 19 ~ "12-19", idade <= 39 ~ "20-39", idade <= 59 ~ "40-59", idade >= 60 ~ "60+", TRUE ~ "ignorado"),
      doenca = tolower(doenca),
      sexo = case_when(CS_SEXO == "M" ~ "masculino", CS_SEXO == "F" ~ "feminino", TRUE ~ "ignorado"),
      calc_atraso = as.numeric(as.Date(DT_INTERNA) - as.Date(DT_SIN_PRI)),
      atraso_dias = if_else(is.na(calc_atraso) | calc_atraso < 0, NA_real_, calc_atraso),
      atraso_proxy = "dias_sintoma_internacao",
      
      # MUDANÇA AQUI: hospitalizado é estritamente TRUE ou FALSE (Isolado de gravidade)
      hospitalizado = if_else(!is.na(DT_INTERNA), TRUE, FALSE),
      obito = if_else(EVOLUCAO %in% c(2, 4), TRUE, FALSE),
      gravidade = case_when(CLASSI_FIN %in% c(10, 11) ~ "leve", CLASSI_FIN == 12 ~ "alarme", CLASSI_FIN == 13 ~ "grave", TRUE ~ NA_character_),
      
      lista_ativas = list(c(
        if(!is.na(DIABETES) && as.character(DIABETES) == "1") "diabetes" else NULL,
        if(!is.na(HIPERTENSA) && as.character(HIPERTENSA) == "1") "hipertensao" else NULL,
        if(!is.na(RENAL) && as.character(RENAL) == "1") "renal" else NULL,
        if(!is.na(HEPATOPAT) && as.character(HEPATOPAT) == "1") "hepatopatia" else NULL,
        if(!is.na(HEMATOLOG) && as.character(HEMATOLOG) == "1") "hematologica" else NULL,
        if(!is.na(AUTO_IMUNE) && as.character(AUTO_IMUNE) == "1") "autoimune" else NULL,
        if(!is.na(ACIDO_PEPT) && as.character(ACIDO_PEPT) == "1") "acido_peptica" else NULL
      )),
      n_comorbidades = length(unlist(lista_ativas)),
      comorbidades = list(list(tem_dado_comorbidade = jsonlite::unbox(TRUE), lista = if(length(unlist(lista_ativas)) == 0) list() else unlist(lista_ativas)))
    ) %>% ungroup() %>%
    mutate(atraso_dias = as.numeric(atraso_dias)) %>%
    # MUDANÇA AQUI: Substituído 'grave_ou_internado' por 'hospitalizado' para persistir na Prata
    select(doenca, uf, ano, idade, faixa_etaria, sexo, atraso_dias, atraso_proxy, gravidade, hospitalizado, obito, n_comorbidades, comorbidades)
  
  if (!is.null(df_clean) && nrow(df_clean) > 0) {
    db$insert(df_clean, na = "null")
  }
}

for (doenca in doencas_comorbidades) {
  pasta_s3 <- if (doenca == "Dengue") "Dengue" else "Chikungunya"
  prefixo_arq <- if (doenca == "Dengue") "DENGBR" else "CHIKBR"
  
  anos_busca <- if (doenca == "Dengue") 2016:2026 else 2016:2026
  
  for (ano in anos_busca) {
    ano_curto <- substr(as.character(ano), 3, 4)
    url_url <- paste0("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/", pasta_s3, "/csv/", prefixo_arq, ano_curto, ".csv.zip")
    
    cat("\n==================================================\n")
    cat("Processando:", doenca, " | Ano:", ano, " (VIA STREAMING/CHUNKS)\n")
    cat("==================================================\n")
    
    tryCatch({
      nome_zip <- paste0(doenca, "_", ano, ".zip")
      nome_csv <- paste0(prefixo_arq, ano_curto, ".csv")
      
      download.file(url_url, destfile = nome_zip, mode = "wb", method = "libcurl")
      
      read_csv_chunked(
        unz(nome_zip, nome_csv),
        callback = DataFrameCallback$new(function(chunk, pos) {
          processar_chunk_dengue_chik(chunk, pos, doenca, ano, mapa_uf_ibge, db)
        }),
        chunk_size = 50000,
        show_col_types = FALSE
      )
      
      file.remove(nome_zip)
      cat("-> Sucesso!", doenca, ano, "salvo de forma segura por lotes.\n")
      gc(); Sys.sleep(1)
    }, error = function(e) { cat("⚠️ Erro crítico ao processar o ano:", ano, "em", doenca, "\nMensagem:", e$message, "\n") })
  }
}

# ==============================================================================
# LOTE 2: ZIKA (Processamento isolado e nativo de 2016 a 2026)
# ==============================================================================
cat("\n==================================================\n")
cat("Processando: Zika (2016-2026)\n")
cat("==================================================\n")

for (ano in 2016:2026) {
  ano_curto <- substr(as.character(ano), 3, 4)
  url_zika <- paste0("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINAN/Zikavirus/csv/ZIKABR", ano_curto, ".csv.zip")
  
  tryCatch({
    nome_zip <- paste0("Zika_", ano, ".zip")
    nome_csv <- paste0("ZIKABR", ano_curto, ".csv")
    
    download.file(url_zika, destfile = nome_zip, mode = "wb", method = "libcurl")
    df_raw <- read_csv(unz(nome_zip, nome_csv), show_col_types = FALSE)
    file.remove(nome_zip)
    
    colunas_obrigatorias_zika <- c("SG_UF_NOT", "NU_ANO", "NU_IDADE_N", "CS_SEXO", "DT_SIN_PRI", "DT_NOTIFIC", "EVOLUCAO")
    missing_cols_zika <- setdiff(colunas_obrigatorias_zika, names(df_raw))
    if(length(missing_cols_zika) > 0) {
      df_raw[missing_cols_zika] <- NA_character_
    }
    
    df_clean <- df_raw %>%
      filter(as.character(NU_ANO) == as.character(!!ano)) %>%
      select(uf = SG_UF_NOT, ano = NU_ANO, NU_IDADE_N, CS_SEXO, DT_SIN_PRI, DT_NOTIFIC, EVOLUCAO) %>%
      rowwise() %>%
      mutate(
        uf = coalesce(unname(mapa_uf_ibge[as.character(uf)]), as.character(uf)),
        nu_idade_str = stringr::str_pad(as.character(NU_IDADE_N), 4, pad = "0"),
        id_tipo = substr(nu_idade_str, 1, 1),
        id_valor = as.numeric(substr(nu_idade_str, 2, 4)),
        idade = case_when(id_tipo == "4" ~ id_valor, id_tipo == "3" ~ id_valor / 12, id_tipo %in% c("1", "2") ~ 0, TRUE ~ NA_real_),
        faixa_etaria = case_when(idade < 1 ~ "<1", idade <= 4 ~ "1-4", idade <= 11 ~ "5-11", idade <= 19 ~ "12-19", idade <= 39 ~ "20-39", idade <= 59 ~ "40-59", idade >= 60 ~ "60+", TRUE ~ "ignorado"),
        doenca = "zika",
        sexo = case_when(CS_SEXO == "M" ~ "masculino", CS_SEXO == "F" ~ "feminino", TRUE ~ "ignorado"),
        calc_atraso = as.numeric(as.Date(DT_NOTIFIC) - as.Date(DT_SIN_PRI)),
        atraso_dias = if_else(is.na(calc_atraso) | calc_atraso < 0, NA_real_, calc_atraso),
        atraso_proxy = "dias_sintoma_notificacao",
        
        # Estrutura limpa: Zika não possui dado de gravidade clínica e internação na ficha
        gravidade = NA_character_, 
        hospitalizado = NA,
        obito = if_else(EVOLUCAO %in% c(2, 4), TRUE, FALSE),
        n_comorbidades = NA_integer_,
        comorbidades = list(list(tem_dado_comorbidade = jsonlite::unbox(FALSE), lista = list()))
      ) %>% ungroup() %>%
      mutate(atraso_dias = as.numeric(atraso_dias)) %>%
      select(doenca, uf, ano, idade, faixa_etaria, sexo, atraso_dias, atraso_proxy, gravidade, hospitalizado, obito, n_comorbidades, comorbidades)
    
    if (nrow(df_clean) > 0) {
      db$insert(df_clean, na = "null")
      cat("-> Sucesso! Zika", ano, "salvo no MongoDB. Linhas:", nrow(df_clean), "\n")
    }
    rm(df_raw, df_clean); gc(); Sys.sleep(1)
  }, error = function(e) { 
    cat("⚠️ Erro ao processar Zika no ano:", ano, "\nMensagem:", e$message, "\n") 
  }) # CORREÇÃO: Adicionado o fechamento correto do bloco tryCatch
}

# ==============================================================================
# LOTE 3: PROCESSAMENTO DE FEBRE AMARELA
# ==============================================================================
cat("\n==================================================\n")
cat("Processando: Febre_Amarela\n")
cat("==================================================\n")
tryCatch({
  url_fa <- "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Febre+Amarela/fa_casoshumanos_1994-2026.csv"
  df_raw <- read_csv2(url_fa, locale = locale(encoding = "ISO-8859-1"), show_col_types = FALSE)
  
  df_clean <- df_raw %>%
    filter(as.character(ANO_IS) >= "2016" & as.character(ANO_IS) <= "2026") %>%
    select(uf = UF_LPI, ano = ANO_IS, SEXO, IDADE, DT_IS, DT_OBITO, OBITO) %>%
    rowwise() %>% 
    mutate(
      uf = coalesce(unname(mapa_uf_ibge[as.character(uf)]), as.character(uf)),
      doenca = "febre_amarela",
      sexo = case_when(SEXO == "M" ~ "masculino", SEXO == "F" ~ "feminino", TRUE ~ "ignorado"),
      idade = as.numeric(IDADE),
      faixa_etaria = case_when(idade < 1 ~ "<1", idade <= 4 ~ "1-4", idade <= 11 ~ "5-11", idade <= 19 ~ "12-19", idade <= 39 ~ "20-39", idade <= 59 ~ "40-59", idade >= 60 ~ "60+", TRUE ~ "ignorado"),
      data_is = lubridate::dmy(DT_IS),
      data_obito = lubridate::dmy(DT_OBITO),
      calc_fa = as.numeric(data_obito - data_is),
      atraso_dias = if_else(is.na(calc_fa) | calc_fa < 0, NA_real_, calc_fa),
      atraso_proxy = "dias_sintoma_obito",
      obito = if_else(OBITO == "SIM" | !is.na(DT_OBITO), TRUE, FALSE),
      
      # Alinhando a estrutura com o Lote 1
      gravidade = NA_character_, 
      hospitalizado = NA,
      n_comorbidades = NA_integer_,
      comorbidades = list(list(tem_dado_comorbidade = jsonlite::unbox(FALSE), lista = list()))
    ) %>% ungroup() %>%
    mutate(atraso_dias = as.numeric(atraso_dias)) %>%
    select(doenca, uf, ano, idade, faixa_etaria, sexo, atraso_dias, atraso_proxy, gravidade, hospitalizado, obito, n_comorbidades, comorbidades)
  
  db$insert(df_clean, na = "null")
  cat("-> Sucesso! Febre Amarela salva de forma robusta. Linhas:", nrow(df_clean), "\n")
  rm(df_raw, df_clean); gc()
}, error = function(e) { cat("⚠️ Erro crítico em Febre Amarela. Mensagem:", e$message, "\n") })

cat("\nVolumetria total final no banco 'arboviroses1':", db$count(), "\n")