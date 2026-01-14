"""
Simple Circuit Breaker implementation.

Usato per limitare chiamate ripetute a servizi esterni che stanno fallendo.
Stati possibili: CLOSED, OPEN, HALF_OPEN.
"""

import time
import threading


class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30, expected_exception=Exception):
        """Inizializza il circuito.

        failure_threshold: numero di fallimenti prima di aprire il circuito
        recovery_timeout: secondi dopo i quali si tenta una chiamata in HALF_OPEN
        expected_exception: tipo di eccezione che conta come fallimento
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        """Esegue `func(*args, **kwargs)` applicando la logica del circuit breaker.

        Se il circuito è OPEN e non è ancora scaduto il timeout, solleva
        `CircuitBreakerOpenException`. Se il timeout è scaduto passa a HALF_OPEN
        e permette una chiamata di prova.
        """
        with self.lock:
            if self.state == 'OPEN':
                # tempo trascorso dall'ultimo fallimento
                time_since_failure = time.time() - (self.last_failure_time or 0)
                if time_since_failure > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                else:
                    raise CircuitBreakerOpenException("Circuit is open. Call denied.")

            try:
                result = func(*args, **kwargs)
            except self.expected_exception as e:
                # incremento contatore di fallimenti
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = 'OPEN'
                raise e
            else:
                # se la chiamata di prova in HALF_OPEN ha successo, chiudo il circuito
                if self.state == 'HALF_OPEN':
                    self.state = 'CLOSED'
                    self.failure_count = 0
                return result


class CircuitBreakerOpenException(Exception):
    """Sollevata quando il circuito è aperto e la chiamata non è permessa."""
    pass

