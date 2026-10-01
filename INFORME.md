# Trabajo Práctico - Coordinación


## Implementacion

A continuacion se detallan los aspectos mas importantes de la solucion propuesta


## Manejo de Multiples Clientes

Para manejar multiples clientes, la solucion asigna un ID al momento en que se recibe un nuevo cliente y se instancia dentro del gateway. Se hace de la siguiente manera:

```go
type MessageHandler struct {
	clientID  uint64
	sentCount uint64
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		clientID:  atomic.AddUint64(&globalClientID, 1),
		sentCount: 0,
	}
}
```

Con este ID las posteriores instancias lo utilizaran para asociar los datos y estados necesarios a cada cliente y asi poder realizar multiples consultas concurrentemente. Por ejemplo, el siguiente struct muestra como una instancia de sum guarda los datos de un determinado cliente

```go
type Sum struct {
	id                      int
	inputQueue              middleware.Middleware
	outputExchange          middleware.Middleware
	coordinator             *Coordinator
	clientFruitItemMap      map[uint64]map[string]fruititem.FruitItem
	completedClients        map[uint64]bool
	clientPendingItemsCount map[uint64]int
	mu                      sync.Mutex
	aggregationAmount       int
	aggregationPrefix       string
}
```

El campo clientFruitTtemMap es el encargado de mantener los datos recibidos de los clientes. De manera analoga se utiliza este mismo mecanismo en las demas instancias de la arquitectura. Cuando se recibe un mensaje de EOF por parte de un cliente, el gateway envia un mensaje de EOF conteniendo el ID de dicho cliente para que las instancias puedan realizar las operaciones necesarias para resolver la consulta. Una vez resulta se liberan los datos y estados asociados a dicho cliente para evitar el llenado de la memoria de las instancias de procesamiento.


## Coordinacion entre Sum, Aggregation y Join

Para la coordinacion entre las distintas etapas se implemento lo siguiente:

### Coordinator

```go
type Coordinator struct {
	id                        int
	controlExchange           middleware.Middleware
	reportedMessagesByClient  map[uint64]map[int]uint64
	processedMessagesByClient map[uint64]uint64
	expectedMessagesByClient  map[uint64]uint64
	mu                        sync.Mutex
	isEOFAnnounced            map[uint64]bool
	completedClients          map[uint64]bool
	onFlushCallback           func(clientID uint64) error
	ctx                       context.Context
	cancel                    context.CancelFunc
	wg                        sync.WaitGroup
}
```

Esta entidad se encarga de coordinar las distintas instancias de sum haciendo uso de los siguientes mecanismos:

- **Exchange de control:** Dado que solo una replica recibira el EOF por parte del gateway, es necesario comunicar a las demas replicas que dicho cliente ya envio todos sus datos asi las demas instancias pueden realizar sus operaciones.

- **Cantidad de mensajes enviados por cliente:** El gateway al enviar un mensaje de EOF ademas de enviar el ID, envia la cantidad de mensajes que recibio del cliente(es el campo sentCount en el struct de message handler) para que la instancia coordinadora sepa anunciar en el exchange que las demas replicas de sum deben enviar sus datos.

- **Mensajes de control:** Son los tipos de mensajes utilizados para coordinar los distintos eventos al momento de recibir el EOF. Son los siguientes

```go
const (
	MsgAnnounceEOF ControlMsgType = "ANNOUNCE_EOF"
	MsgReportCount ControlMsgType = "REPORT_COUNT"
	MsgCommitFlush ControlMsgType = "COMMIT_FLUSH"
)
```

El funcionamiento de el coordinador es el siguiente. Sum al recibir  el EOF se convierte en coordinador. Primero setea la variable isEOFAnnounced a true asociando el id del cliente recibido, luego anuncia a las demas instancias de sum, con el mensaje MsgAnnounceEOF, que deben notificar cuantos mensajes procesaron de dicho cliente(el campo processedMessagesByClient es donde se guarda esta informacion). Las instancias setean su propia variable  isEOFAnnounced a true y responden con el mensaje MsgReportCount informando la cantidad de mensajes que se procesaron. La instancia coordinadora al recibirlos compara la cantidad total de mensajes procesados, si esta es igual a la informada por el gateway se hace envio del mensaje MsgCommitFlush para que todas las instancias de sum envien sus datos a las instancias de aggregacion y se libreren los recursos asociados. En caso contrario, el coordinador espera a que se complete la cantidad esperada. Esto sucedera cuando la instancia que aun no proceso la totalidad de sus mensajes, reciba datos a procesar, compare que isEOFAnnounced es true y notifique nuevamente al coordinador con un mensaje de tipo MsgReportCount la cantidad de mensajes procesados. Esto se repite hasta alcanzar la cantidad esperada enviada por el gateway.

### Coordinacion en Aggregation

```go
type Aggregation struct {
	outputQueue        middleware.Middleware
	inputExchange      middleware.Middleware
	sumAmount          int
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
	clientCompletedMap map[uint64]int
	topSize            int
}
```

Una vez que las instancias de sum envian sus datos, envian ademas, un mensaje de EOF con el id del cliente. Las instancias de aggregation llevan un registro de cuantos EOF recibieron por cliente(campo clientCompletedMap), si dicha cantidad de mensajes de EOF recibidos es igual a sumAmount(indica la cantidad de instancias de sum) procede a realizar el top de frutas para posteriormente enviarlas a la etapa de Join y liberan los recursos asociados a dicho cliente.

### Coordinacion en Join

```go
type Join struct {
	inputQueue         middleware.Middleware
	outputQueue        middleware.Middleware
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
	clientCompletedMap map[uint64]int
	topSize            int
	aggregationAmount  int
}
```

Funciona de manera analoga a la etapa de agregacion. Cada instancia de aggregacion envia un EOF con el id del cliente. Join al recibirlas las guarda en clientCompletedMap, si dicha cantidad es igual a agregationAmount, realiza el computo, libera los recursos asociados a dicho cliente y envia los datos al gateway para terminar la request.


## Manejo de grandes volumenes de datos

Para el manejo de grandes volumenes de datos, se obto por una estrategia en la cual las instancias de sum procesan hasta un limite de n mensajes siendo n una cantidad definida. Alcanzada dicha cota las instancias envian estos datos a la etapa de agregation para que esta realice computo y liberan los recursos asociados para evitar llenar la memoria de la aplicacion. Para evitar consumo innecesario de recursos cada instancia agrupa y envia los datos por batches para reducir la cantidad de mensajes enviados por la cola. Esto se repite hasta que llegue un EOF con el id asociado a la request del cliente.

## Distribucion de procesamiento entre las etapas de aggregacion

Para solucionar el problema de procesamiento redundante en las etapas de agregacion, en sum se realizo una particion por hash, asignando un id a cada una de las instancias de aggregacion para que cada una solo compute los tops parciales de las frutas correspondientes a cada instancia. De esta manera se evita que cada instancia de agregacion tenga en su mapa de frutas, la totalidad de las frutas dentro del set de datos diviendo asi entre cada instancia la cantidad de memoria para almacenar los datos procesados por cada etapa de sum. A continuacion se muestra la porcion de codigo encargada de realizar dicha particion y posterior envio.

```go
func (sum *Sum) sendBatchItems(clientID uint64, items map[string]fruititem.FruitItem) error {
	partitionBatches := make(map[int][]fruititem.FruitItem)

	for _, item := range items {
		h := fnv.New32a()
		h.Write([]byte(item.Fruit))
		targetID := int(h.Sum32()) % sum.aggregationAmount
		partitionBatches[targetID] = append(partitionBatches[targetID], item)
	}

	for targetID, records := range partitionBatches {
		if len(records) == 0 {
			continue
		}
		message, err := inner.SerializeMessage(clientID, records)
		if err != nil {
			slog.Error("While serializing batch message", "err", err)
			return err
		}

		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, targetID)
		if err := sum.outputExchange.SendTo(routingKey, *message); err != nil {
			slog.Error("While sending batch message", "err", err)
			return err
		}
	}
	return nil
}
```

Cabe destacar que se extendio la interfaz del middleware con el metodo SendTo() para poder asi enviar mensajes a una determinada cola asociada a la routingKey.



























