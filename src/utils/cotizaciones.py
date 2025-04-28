import requests, logging, sys
import src.utils.log_config
from config.config import URL_API_OER, API_KEY_OER

def obtener_cotizaciones(simbolo:str):
    ''' 
     Obtiene la cotización de una moneda local respecto al USD desde la API de OpenExchangeRates.
    - Args:
        simbolo (str): Código de la moneda (ej. 'ARS', 'UYU', 'PEN')
    - Returns:
        float: Valor de 1 USD en la moneda local especificada
    '''
    
    try:
        logging.info(f'Consultando la cotización para "{simbolo}"')
        url = f'{URL_API_OER}?app_id={API_KEY_OER}&symbols={simbolo}'
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        rate = data['rates'].get(simbolo)
        
        logging.info(f'Cotización obtenida: 1 USD = {rate} {simbolo}')
        return rate
    
    except requests.exceptions.RequestException as req_err:
        logging.error(f'Error de red al consultar cotización para: "{simbolo}": {req_err}')
        raise
    except Exception as e:
        logging.error(f'Error inesperado al obtener cotización para "{simbolo}": {e}')
        raise

if __name__ == '__main__':
    if len(sys.argv) > 1:
        simbolo = sys.argv[1]
    taza = obtener_cotizaciones(simbolo)
    print(taza)
        