#!/bin/bash

echo "🔍 Diagnosticando problema con dbt ls..."

echo "=== Paso 1: Verificar archivos en models ==="
echo "📁 Contenido de models:"
ls -la models/

echo "=== Paso 2: Verificar sintaxis de modelos ==="
for file in models/*.sql; do
    if [ -f "$file" ]; then
        echo "📄 Verificando: $file"
        head -5 "$file"
        echo "---"
    fi
done

echo "=== Paso 3: Crear modelo simple de prueba ==="
cat > models/simple_test.sql << EOF
-- Modelo simple de prueba
SELECT 1 as test_column
EOF

echo "✅ Modelo simple_test.sql creado"

echo "=== Paso 4: Probar dbt ls ==="
dbt ls

echo "=== Paso 5: Probar compilación ==="
dbt compile --select simple_test

echo "=== Paso 6: Probar ejecución ==="
dbt run --select simple_test

echo "✅ Diagnóstico completado!"
echo "💡 Si dbt ls sigue sin mostrar modelos, ejecuta:"
echo "   dbt debug"
echo "   dbt compile" 