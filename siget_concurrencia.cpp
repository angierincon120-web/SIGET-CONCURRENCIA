// // Tarea de Sistemas Operativos - filósofos comensales adaptado al SIGET

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <chrono>
#include <random>
#include <atomic>
#include <sstream>

const int NUM_CONTROLADORES = 5;   // número de intersecciones (hilos)
const int CICLOS_POR_CONTROLADOR = 4; // cuántas veces cada uno "ajusta semáforos"

enum Estado { PENSANDO, HAMBRIENTO, AJUSTANDO };
// PENSANDO   = el controlador está en operación normal, no necesita nada especial
// HAMBRIENTO = quiere ajustar semáforos y está esperando las 2 zonas
// AJUSTANDO  = tiene el control de sus 2 zonas y está ajustando semáforos

class MonitorTrafico {
public:
    MonitorTrafico(int n)
        : n(n), estado(n, PENSANDO), cv(n)
    {}

    // Un controlador llama esto cuando necesita ajustar semáforos.
    // Se bloquea (sin gastar CPU) hasta que el monitor le da permiso.
    void solicitar_zonas(int id) {
        std::unique_lock<std::mutex> lock(mtx); // entra al monitor (exclusión mutua)
        estado[id] = HAMBRIENTO;
        log(id, "quiere ajustar semáforos (solicita sus 2 zonas de confluencia)");
        probar(id); // intenta pasar a AJUSTANDO de inmediato si es posible
        // Si "probar" no lo dejó en AJUSTANDO, espera aquí liberando el mutex
        // hasta que otro hilo lo despierte con notify (ver liberar_zonas)
        cv[id].wait(lock, [this, id] { return estado[id] == AJUSTANDO; });
    }

    // Un controlador llama esto cuando termina de ajustar semáforos.
    void liberar_zonas(int id) {
        std::unique_lock<std::mutex> lock(mtx);
        estado[id] = PENSANDO;
        log(id, "liberó sus 2 zonas de confluencia");
        // Al soltar sus zonas, puede que ahora sus vecinos sí puedan operar
        probar(izquierda(id));
        probar(derecha(id));
    }

private:
    int n;
    std::vector<Estado> estado;
    std::vector<std::condition_variable> cv;
    std::mutex mtx;

    int izquierda(int id) { return (id + n - 1) % n; }
    int derecha(int id)   { return (id + 1) % n; }


    void probar(int id) {
        if (estado[id] == HAMBRIENTO &&
            estado[izquierda(id)] != AJUSTANDO &&
            estado[derecha(id)]   != AJUSTANDO) {
            estado[id] = AJUSTANDO;
            log(id, "obtuvo control de sus 2 zonas -> AJUSTANDO semáforos");
            cv[id].notify_one(); // despierta a ese hilo, ya puede continuar
        }
    }


    void log(int id, const std::string& msg) {
        static std::mutex log_mtx;
        std::lock_guard<std::mutex> lg(log_mtx);
        std::cout << "[Controlador-" << id << "] " << msg << std::endl;
    }
};


std::mutex stats_mtx;
int ajustes_totales = 0;

void controlador_interseccion(int id, MonitorTrafico& monitor) {
    std::mt19937 rng(id * 1000 + 42); // semilla distinta por hilo
    std::uniform_int_distribution<int> tiempo_espera(100, 400);
    std::uniform_int_distribution<int> tiempo_ajuste(150, 350);

    for (int ciclo = 0; ciclo < CICLOS_POR_CONTROLADOR; ++ciclo) {
        // "PENSANDO": operación normal, simulado con una pequeña espera
        std::this_thread::sleep_for(std::chrono::milliseconds(tiempo_espera(rng)));

        // Pide permiso al monitor para tomar sus 2 zonas de confluencia
        monitor.solicitar_zonas(id);

        // Sección crítica: está AJUSTANDO semáforos (tiene sus 2 zonas)
        std::this_thread::sleep_for(std::chrono::milliseconds(tiempo_ajuste(rng)));

        {
            std::lock_guard<std::mutex> lg(stats_mtx);
            ajustes_totales++;
        }

        // Libera sus 2 zonas para que otros controladores puedan usarlas
        monitor.liberar_zonas(id);
    }
}

// ------------------------------------------------------------------
// main
// ------------------------------------------------------------------
int main() {
    std::cout << "=== SIGET: simulación de concurrencia (filósofos comensales) ===" << std::endl;
    std::cout << "Controladores de intersección: " << NUM_CONTROLADORES << std::endl;
    std::cout << "Ciclos de ajuste por controlador: " << CICLOS_POR_CONTROLADOR << std::endl;
    std::cout << "===================================================================" << std::endl;

    MonitorTrafico monitor(NUM_CONTROLADORES);
    std::vector<std::thread> hilos;

    for (int i = 0; i < NUM_CONTROLADORES; ++i) {
        hilos.emplace_back(controlador_interseccion, i, std::ref(monitor));
    }

    for (auto& h : hilos) {
        h.join(); // espera a que todos los controladores terminen
    }

    std::cout << "===================================================================" << std::endl;
    std::cout << "Simulación finalizada. Total de ajustes de semáforos realizados: "
              << ajustes_totales << std::endl;
    std::cout << "Esperado: " << NUM_CONTROLADORES * CICLOS_POR_CONTROLADOR
              << " (ningún proceso se quedó bloqueado para siempre -> sin interbloqueo)" << std::endl;

    return 0;
}