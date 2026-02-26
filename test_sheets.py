"""
Script para testar a atualização do Google Sheet diretamente no terminal.
Execute: python test_sheets.py
"""
import sys
import os

# Garante que o .env seja carregado
os.chdir(os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

print("=" * 50)
print("Teste de atualização do Google Sheet")
print("=" * 50)

# Verifica variáveis
spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
if not spreadsheet_id:
    print("\nERRO: GOOGLE_SHEETS_SPREADSHEET_ID não está no .env")
    sys.exit(1)
print(f"\nSpreadsheet ID: {spreadsheet_id}")

creds = os.getenv("GA4_CREDENTIALS_FILE") or os.getenv("GOOGLE_SHEETS_CREDENTIALS")
if not creds or len(creds) < 50:
    print("ERRO: GA4_CREDENTIALS_FILE ou GOOGLE_SHEETS_CREDENTIALS não configurado")
    sys.exit(1)
print("Credenciais: OK")

print("\nExecutando update_sheet()...\n")

try:
    import google_sheets_sales as gs
    added, msg = gs.update_sheet()
    print("\n" + "=" * 50)
    print(f"RESULTADO: {msg}")
    print(f"Linhas adicionadas: {added}")
    print("=" * 50)
except Exception as e:
    print("\n" + "=" * 50)
    print(f"ERRO: {e}")
    import traceback
    traceback.print_exc()
    print("=" * 50)
    sys.exit(1)
