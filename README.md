# Istruzioni per l'esecuzione del codice

È necessario inserire un file .env nella directory principale. Il suddetto file verrà inviato via email per semplicità di configurazione.
Tra le variabili d'ambiente è necessario inserire nei campi SENDER_EMAIL ed EMAIL_PASSWORD, rispettivamente una mail (GMAIL) e la relativa password per applicazioni (16 caratteri).
La password per applicazioni viene ottenuta andando nel proprio account Google->Sicurezza->Autenticazione a due fattori. Dopo aver attivato l'autenticazione a due fattori, digitare nella barra di ricerca in alto "password per applicazioni".

È necessario inserire il file credentials.json, scaricabile sul sito di OpenSky dopo essersi registrati, nella directory data_collector.

Una volta effettuate queste operazioni basterà posizionarsi nella root del progetto e digitare da un terminale i comandi:

```bash
chmod +x start-cluster.sh
./start-cluster.sh
```

Per accedere tramite browser a kafka-ui (localhost:8080) bisogna effettuare il port forwarding:

```bash
kubectl port-forward service/kafka-ui 8080:8080
```
Per accedere tramite browser all'interfaccia di prometheus (localhost:9090) bisogna effettuare il port forwarding:

```bash
kubectl port-forward service/prometheus-service 9090:9090
```

Per eliminare il cluster bisognerà inserire il seguente comando:

```bash
kind delete cluster --name migration-cluster
```

## Testing
Nella repository è presente un json contenente la collezione da importare su Postman per effettuare i test.
