#!/bin/bash

# 🚀 SCRIPT DE SETUP AUTOMÁTICO PARA CLOUD SHELL
# Bank Marketing Data Mart - dbt + BigQuery + Python

set -e  # Exit on any error

echo "🏦 BANK MARKETING DATA MART - CLOUD SHELL SETUP"
echo "================================================"

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para logging
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Paso 1: Verificar que estamos en Cloud Shell
log_info "Verificando entorno Cloud Shell..."

if [ -z "$CLOUD_SHELL" ]; then
    log_warning "No se detectó Cloud Shell. Algunas funciones pueden no funcionar correctamente."
else
    log_success "Cloud Shell detectado"
fi

# Paso 2: Configurar variables de entorno
log_info "Configurando variables de entorno..."

# Obtener PROJECT_ID
PROJECT_ID=$(gcloud config get-value project 2>/dev/null || echo "")
if [ -z "$PROJECT_ID" ]; then
    log_error "No se pudo obtener PROJECT_ID. Asegúrate de estar autenticado en gcloud."
    exit 1
fi

export PROJECT_ID=$PROJECT_ID
export DATASET_NAME="bank_marketing_dm"

log_success "PROJECT_ID: $PROJECT_ID"
log_success "DATASET_NAME: $DATASET_NAME"

# Paso 3: Verificar autenticación
log_info "Verificando autenticación con Google Cloud..."

if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    log_error "No hay cuentas activas. Ejecuta 'gcloud auth login' primero."
    exit 1
fi

log_success "Autenticación verificada"

# Paso 4: Habilitar APIs necesarias
log_info "Habilitando APIs de BigQuery..."

gcloud services enable bigquery.googleapis.com --quiet
gcloud services enable bigqueryconnection.googleapis.com --quiet

log_success "APIs habilitadas"

# Paso 5: Instalar dependencias de Python
log_info "Instalando dependencias de Python..."

pip install --upgrade pip
pip install -r requirements.txt

log_success "Dependencias de Python instaladas"

# Paso 6: Configurar dbt
log_info "Configurando dbt..."

# Crear directorio de configuración
mkdir -p ~/.dbt

# Copiar archivo de configuración
cp profiles.yml ~/.dbt/

log_success "dbt configurado"

# Paso 7: Verificar configuración dbt
log_info "Verificando configuración de dbt..."

if dbt debug; then
    log_success "Configuración de dbt verificada"
else
    log_error "Error en la configuración de dbt"
    exit 1
fi

# Paso 8: Instalar dependencias dbt
log_info "Instalando dependencias de dbt..."

dbt deps

log_success "Dependencias de dbt instaladas"

# Paso 9: Cargar datos
log_info "Cargando datos iniciales..."

python scripts/data_loader.py

if [ $? -eq 0 ]; then
    log_success "Datos cargados exitosamente"
else
    log_error "Error al cargar datos"
    exit 1
fi

# Paso 10: Compilar modelos
log_info "Compilando modelos dbt..."

dbt compile

log_success "Modelos compilados"

# Paso 11: Ejecutar modelos
log_info "Ejecutando modelos dbt..."

dbt run

log_success "Modelos ejecutados"

# Paso 12: Ejecutar pruebas
log_info "Ejecutando pruebas de calidad..."

dbt test

log_success "Pruebas ejecutadas"

# Paso 13: Generar documentación
log_info "Generando documentación..."

dbt docs generate

log_success "Documentación generada"

# Paso 14: Mostrar resumen
echo ""
echo "🎉 SETUP COMPLETADO EXITOSAMENTE"
echo "================================"
echo ""
echo "📊 DATOS DISPONIBLES:"
echo "   - Raw Data: $PROJECT_ID.bank_marketing_dm.bank_marketing"
echo "   - Staging: $PROJECT_ID.bank_marketing_dm_dev.stg_bank_marketing"
echo "   - Marts: $PROJECT_ID.bank_marketing_dm_dev.fct_marketing_kpis"
echo ""
echo "🔧 COMANDOS ÚTILES:"
echo "   - dbt run          # Ejecutar todos los modelos"
echo "   - dbt test         # Ejecutar pruebas"
echo "   - dbt docs serve   # Ver documentación"
echo "   - dbt compile      # Compilar sin ejecutar"
echo ""
echo "📈 PRÓXIMOS PASOS:"
echo "   1. Revisar documentación: dbt docs serve"
echo "   2. Explorar datos en BigQuery Console"
echo "   3. Ejecutar consultas de análisis"
echo "   4. Configurar CI/CD en GitHub"
echo ""

log_success "¡Setup completado! El proyecto está listo para usar." 