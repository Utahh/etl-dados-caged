import sys
sys.path.append('.') 
from src.ftp_client import FTPClient
from src.processor import CagedProcessor

def main():
    print("\n=== EXTRATOR CAGED 2.0 (Histórico + Enriquecimento) ===")
    
    year = input("Ano (YYYY): ").strip()
    month = input("Mês (MM): ").strip()
    
    if len(year) != 4 or not year.isdigit(): return
    month = month.zfill(2)

    # 1. Download (Com rotação de histórico automática)
    ftp = FTPClient()
    zip_path = ftp.download_caged(year, month)
    
    if not zip_path: return

    # 2. Extração e Processamento
    processor = CagedProcessor()
    # Extrai para pasta separada 'extracted'
    txt_path = processor.extract_file(zip_path)
    
    if txt_path:
        output_filename = f"CAGED_SP_{year}_{month}_Enriquecido.csv"
        # Passamos ano e mês para criar a coluna data_ref
        processor.process_data(txt_path, output_filename, year, month)
        
        # Opcional: Limpar o arquivo extraído (TXT) para economizar espaço, 
        # já que o ZIP está salvo no histórico.
        import os
        try:
            os.remove(txt_path)
            print("🧹 Arquivo TXT temporário removido (ZIP mantido no histórico).")
        except: pass

if __name__ == "__main__":
    main()