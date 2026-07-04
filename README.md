# BrokerSim

Simulación de un **broker de mensajes** con arquitectura **Cliente–Servidor**, implementada desde cero en **C# / .NET 8**, sin usar RabbitMQ, Kafka, ActiveMQ, Redis Pub/Sub ni ninguna plataforma de mensajería existente.

El servidor implementa toda la lógica del broker (registro de clientes, suscripciones y distribución de mensajes) sobre **WebSockets propios** (`System.Net.WebSockets`, sin SignalR), usando un patrón **Publish/Subscribe por tópicos**.

## 1. Descripción del proyecto

- **Servidor Broker**: aplicación ASP.NET Core que acepta conexiones WebSocket, registra clientes como *productores* o *consumidores*, gestiona suscripciones a tópicos y distribuye los mensajes publicados a los suscriptores correspondientes.
- **Cliente Productor**: se conecta al broker y publica mensajes indicando el tópico destino. Recibe una confirmación (`ack`) del broker por cada mensaje enviado.
- **Cliente Consumidor**: se conecta al broker, se suscribe a uno o más tópicos, y recibe los mensajes publicados en ellos.

Hay dos formas de usar cada rol:
- **Cliente de consola** (`BrokerSim.Client`): productor o consumidor, controlado por comandos de texto.
- **Panel web** (servido por el propio servidor en `http://localhost:5080/`): una demo de **sistema de pedidos de restaurante** con 3 vistas dentro de la misma página — Caja, Cocina y Delivery — construida con WebSocket nativo del navegador (sin librerías externas). Es una capa de presentación sobre el mismo broker genérico; no cambia el protocolo ni el servidor.

### Demo de pedidos (panel web)

| Vista | Rol en el broker | Tópicos | Acción |
|---|---|---|---|
| **Caja** | Productor | Publica en `pedido.cocina` | Registra un pedido (cliente, producto, cantidad, dirección) |
| **Cocina** | Consumidor (y productor) | Se suscribe a `pedido.cocina`, publica en `pedido.delivery` | Recibe el pedido y lo marca "listo" |
| **Delivery** | Consumidor (y productor) | Se suscribe a `pedido.delivery`, publica en `pedido.finalizado` | Recibe el pedido listo y lo marca "entregado" |

El broker no restringe qué puede hacer cada rol (`register` solo es informativo para el log); Cocina y Delivery se registran como `consumer` pero igual pueden publicar — así se comportaría un servicio real que consume de un tópico y produce hacia el siguiente paso del flujo.

Cada pedido viaja como JSON en el campo `content` del protocolo (`{ id, cliente, producto, cantidad, direccion }`), de modo que la información completa (incluida la dirección) esté disponible en cada etapa sin tener que consultar una base de datos aparte.

**Para la demo:** abre 3 pestañas en `http://localhost:5080/`, elige un rol distinto en cada una (los botones Caja/Cocina/Delivery arriba a la izquierda) y sigue el flujo: registra un pedido en Caja → aparece automáticamente en Cocina → al marcarlo "listo" aparece en Delivery → al marcarlo "entregado" se publica `pedido.finalizado`. Cada vista tiene un registro técnico colapsable (▸ *Ver registro técnico*) con los mensajes reales del broker, útil para mostrar el protocolo subyacente durante la exposición.

## 2. Arquitectura propuesta

Ver diagrama completo en [`docs/architecture.svg`](docs/architecture.svg).

```
 Productores                  Servidor Broker                 Consumidores
 (consola / web)              (ASP.NET Core)                  (consola / web)

 ┌────────────┐   WebSocket   ┌───────────────────────────┐   WebSocket   ┌────────────┐
 │ Productor 1├──────────────▶│ Registro de clientes       │──────────────▶│ Consumidor │
 └────────────┘               │ Tabla de suscripciones     │               │ ("noticias")│
 ┌────────────┐               │ por tópico                 │               └────────────┘
 │ Productor 2├──────────────▶│ Distribuidor de mensajes   │──────────────▶┌────────────┐
 └────────────┘               │ Log de eventos en consola  │               │ Consumidor │
                               └───────────────────────────┘               │ ("alertas") │
                                                                            └────────────┘
```

**Flujo de un mensaje:**

1. Un cliente se conecta al servidor por WebSocket y envía `register` indicando su rol (`producer` o `consumer`). El broker le asigna un `clientId`.
2. Un consumidor envía `subscribe` con el tópico que le interesa. El broker lo agrega a la tabla de suscripciones de ese tópico.
3. Un productor envía `publish` con `topic` y `content`. El broker responde con `ack` (confirmación de recepción).
4. El broker busca en la tabla de suscripciones quiénes están suscritos a ese tópico y les reenvía un mensaje `deliver` a cada uno, de forma concurrente.
5. Al desconectarse un cliente, el broker lo elimina del registro y de todas sus suscripciones.

### Protocolo (mensajes JSON sobre WebSocket)

| Dirección | Tipo | Campos relevantes | Descripción |
|---|---|---|---|
| Cliente → Servidor | `register` | `role` | Registra al cliente como productor o consumidor |
| Cliente → Servidor | `subscribe` | `topic` | Suscribe al cliente a un tópico |
| Cliente → Servidor | `unsubscribe` | `topic` | Cancela la suscripción a un tópico |
| Cliente → Servidor | `publish` | `topic`, `content` | Publica un mensaje en un tópico |
| Servidor → Cliente | `registered` | `clientId`, `role` | Confirma el registro |
| Servidor → Cliente | `subscribed` / `unsubscribed` | `topic` | Confirma la (des)suscripción |
| Servidor → Cliente | `ack` | `messageId`, `topic` | Confirma que el broker recibió el mensaje publicado |
| Servidor → Cliente | `deliver` | `topic`, `content`, `from`, `messageId` | Entrega un mensaje a un suscriptor |
| Servidor → Cliente | `error` | `reason` | Reporta un error de protocolo |

### Concurrencia

- El broker corre como **singleton** en el contenedor de dependencias de ASP.NET Core; una única instancia compartida por todas las conexiones.
- Cada conexión WebSocket se atiende en su propio `Task` de recepción.
- Las estructuras de registro y suscripciones usan `ConcurrentDictionary` para permitir acceso simultáneo sin bloqueos manuales.
- Cada `ClientSession` protege sus escrituras al socket con un `SemaphoreSlim`, porque `WebSocket` no admite `SendAsync` concurrente desde varios hilos.
- La distribución a múltiples suscriptores se hace con `Task.WhenAll`, en paralelo.

## 3. Tecnologías utilizadas

- **.NET 8** / C# 12
- **ASP.NET Core** (solo como *host* HTTP + middleware de WebSockets, no como framework de mensajería)
- **System.Net.WebSockets** (servidor) y **System.Net.WebSockets.Client** (cliente de consola)
- **System.Text.Json** para el protocolo de mensajes
- HTML + JavaScript vanilla (panel web, sin frameworks ni librerías)

## 4. Estructura de la solución

```
BrokerSim.sln
src/
  BrokerSim.Protocol/   Librería compartida: DTO del mensaje y tipos del protocolo
  BrokerSim.Server/     Servidor broker (ASP.NET Core) + panel web (wwwroot/)
  BrokerSim.Client/     Cliente de consola (productor o consumidor)
docs/
  architecture.svg      Diagrama de arquitectura
  informe.md            Informe breve (funcionamiento, flujo, problemas, mejoras)
```

## 5. Instalación

Requisitos: **.NET 8 SDK** ([descarga](https://dotnet.microsoft.com/download/dotnet/8.0)).

```bash
git clone <repo>
cd BrokerSim
dotnet restore BrokerSim.sln
```

En Visual Studio: abrir `BrokerSim.sln` directamente; restaura los paquetes automáticamente.

## 6. Ejecución

### Opción A — Visual Studio (recomendada para la demo)

1. Clic derecho en la solución → **Configurar proyectos de inicio** → **Varios proyectos de inicio**.
2. Poner `BrokerSim.Server` en **Iniciar** (acción "Iniciar").
3. Ejecutar (F5). Se abre la consola del servidor escuchando en `http://localhost:5080`.
4. Para cada cliente adicional (productor o consumidor), clic derecho en `BrokerSim.Client` → **Depurar → Iniciar nueva instancia**. Repetir tantas veces como clientes se necesiten.

### Opción B — línea de comandos

Terminal 1 (servidor):
```bash
dotnet run --project src/BrokerSim.Server/BrokerSim.Server.csproj
```

Terminal 2, 3, 4... (uno por cada cliente):
```bash
dotnet run --project src/BrokerSim.Client/BrokerSim.Client.csproj
```

Cada cliente te pregunta la URL del broker (Enter para usar el valor por defecto `ws://localhost:5080/ws`) y el rol (`productor` o `consumidor`).

### Opción C — panel web

Con el servidor corriendo, abrir en el navegador:

```
http://localhost:5080/
```

Elegir vista (Caja / Cocina / Delivery); cada una conecta y se suscribe automáticamente al tópico que le corresponde.

## 7. Ejemplo de uso (el mismo escenario del enunciado)

1. Iniciar el servidor.
2. Abrir dos clientes consumidores:
   - Consumidor 1: `sub noticias`
   - Consumidor 2: `sub alertas`
3. Abrir un cliente productor y escribir:
   ```
   noticias: Hay lluvias fuertes en Lima
   ```
4. El broker:
   - Muestra en consola `[RECIBIDO]` y `[DISTRIBUIDO]`.
   - Envía `ack` al productor.
   - Entrega el mensaje **solo** al Consumidor 1 (suscrito a "noticias"); el Consumidor 2 no lo recibe.

Salida esperada en consola del servidor:
```
[CONECTADO]   Cliente 'a1b2c3d4' conectado como consumer
[CONECTADO]   Cliente 'e5f6a7b8' conectado como consumer
[SUSCRITO]    Cliente 'a1b2c3d4' se suscribió al tópico 'noticias'
[SUSCRITO]    Cliente 'e5f6a7b8' se suscribió al tópico 'alertas'
[CONECTADO]   Cliente 'c9d0e1f2' conectado como producer
[RECIBIDO]    'c9d0e1f2' publicó en tópico 'noticias': "Hay lluvias fuertes en Lima"
[DISTRIBUIDO] Mensaje 7c5dcc del tópico 'noticias' entregado a 1 suscriptor(es)
```

### Ejemplo de uso — demo de pedidos (panel web)

1. Iniciar el servidor.
2. Abrir 3 pestañas en `http://localhost:5080/`: una en **Cocina**, otra en **Delivery**, otra en **Caja**.
3. En Caja, registrar un pedido (cliente, producto, cantidad, dirección).
4. El pedido aparece automáticamente en Cocina (tópico `pedido.cocina`).
5. En Cocina, clic en "Marcar como listo" → el pedido aparece automáticamente en Delivery (tópico `pedido.delivery`).
6. En Delivery, clic en "Marcar como entregado" → se publica en `pedido.finalizado`.

Verificado extremo a extremo (ver `docs/informe.md`):
```
CAJA ack: pedido.cocina
COCINA recibe: {'id': 1001, 'cliente': 'Juan Pérez', 'producto': 'Hamburguesa', 'cantidad': 2, 'direccion': 'Av. Lima 123'}
COCINA ack: pedido.delivery
DELIVERY recibe: {'id': 1001, 'cliente': 'Juan Pérez', 'producto': 'Hamburguesa', 'cantidad': 2, 'direccion': 'Av. Lima 123'}
DELIVERY ack: pedido.finalizado
```

## 8. Pruebas realizadas

El escenario anterior fue verificado extremo a extremo (servidor + clientes de consola + protocolo JSON) confirmando que:
- Un consumidor solo recibe mensajes de los tópicos a los que está suscrito.
- El productor recibe el `ack` de confirmación por cada publicación.
- El servidor conecta/desconecta múltiples clientes concurrentemente sin errores.

Ver detalle en [`docs/informe.md`](docs/informe.md).
