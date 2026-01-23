import sys
import logging
from app import app
from tasks import update_database

# Configurazione Logger per vedere l'output su Kubernetes
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("🕒 [CRONJOB] Avvio procedura aggiornamento voli...")

    # Creiamo il contesto dell'applicazione manualmente.
    # Questo connette SQLAlchemy al DB e inizializza Kafka Producer
    # usando le stesse variabili d'ambiente del server principale.
    with app.app_context():
        try:
            # Passiamo l'oggetto app se la tua funzione lo richiede, 
            # altrimenti basta chiamarla se usa 'current_app' o 'extensions'
            success = update_database()
            if not success:
                logger.error("❌ [CRONJOB] Procedura terminata con errori.")
                sys.exit(1) # Segnala a K8s che è fallito (così fa il retry)

            logger.info("✅ [CRONJOB] Procedura completata con successo.")
            sys.exit(0)
            
        except Exception as e:
            logger.error(f"❌ [CRONJOB] Errore critico: {str(e)}")
            sys.exit(1) # Segnala a K8s che è fallito (così fa il retry)