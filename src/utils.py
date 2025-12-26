def get_month_name(month_number: str) -> str:
    """
    Converte número do mês (ex: '09' ou '9') para nome por extenso (ex: 'Setembro').
    O FTP do governo geralmente usa nomes capitalizados.
    """
    months = {
        '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março', '04': 'Abril',
        '05': 'Maio', '06': 'Junho', '07': 'Julho', '08': 'Agosto',
        '09': 'Setembro', '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
    }
    # Garante que tenha 2 dígitos (ex: '9' vira '09')
    key = str(month_number).zfill(2)
    return months.get(key, None)