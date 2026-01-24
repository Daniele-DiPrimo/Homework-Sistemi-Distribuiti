import sys
import logging
from app import app
from tasks import update_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("🕒 [CRONJOB] Avvio procedura aggiornamento voli...")

    with app.app_context():
        try:
            success = update_database()
            if not success:
                logger.error("❌ [CRONJOB] Procedura terminata con errori.")
                sys.exit(1) 
            logger.info("✅ [CRONJOB] Procedura completata con successo.")
            sys.exit(0)
            
        except Exception as e:
            logger.error(f"❌ [CRONJOB] Errore critico: {str(e)}")
            sys.exit(1) 