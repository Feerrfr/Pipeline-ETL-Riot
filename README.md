# Analisis a un jugador de League Of Legends ETL: [Link](https://pipelinetop.streamlit.app/)
## Descripcion del proyecto
La idea principal del proyecto es recopilar todas las partidas de un jugador en especial, para poder entender como se dessarrolla competitivamente. Para esto hago uso de la API de riotgames, lamentablemente tiene que ser actualizada cada 24hs, con la finalidad de recopilar los datos principales del usuario y lo mas importante sus partidas jugadas.

### AVISO: Seguramente la API_KEY no este actualizada para cuando se quiera abrir por una cuestion de la API que necesita ser cambiada cada 24 hs de igual manera se pueden correr las transformaciones y el analisis de datos GRACIASSS!!! Tambien que si la app se encuentra inactiva suele tardar un poco en ser asignada una maquina virtual.

## Proceso

### Extraccion de datos
Para esta primera etapa se hacen 22 llamados a la api, los primeros 2 llamados son para solicitar la informacion y guardarla de forma full. Para esto se almacenan los datos en `usuario_json` y luego es convertido en un data frame `perfil_df` para luego ser guardado en minIO.

Luego de esto se aprovecha la informacion extraida de `usuario_json` para extraer su **puuid** que es algo muy importante dentro de la api de riotgames ya que es la forma en la que diferencia a sus jugadores mas alla del tag principal, una foreign key.

Haciendo uso del **puuid** es como accederemos, a las ultimas 20 partidas de nuestro jugador analisado. Entonces se hace un llamado mas a la API para obtener los **matchId**.

Ya con nuestra lista de **matchId** estamos listos para hacer los llamados a la API que nos daran toda la informacion para analisar toda la informacion de nuestro jugador, haciendo uso de un for se recorre los **matchId** y a la vez se van haciendo consultas a la api sobre cada partida, se almacenan los datos convertidos en tablas y se mergean entre si para formar una delta table.

Una vez que tenemos toda la informacion extraida, se guarda en minIO, usando la configuracion de `pipeline.conf`

### Transformaciones
La segunda etapa consiste leer los datos bronce de minIO, corroborra si ya existe y determina si hay nueva data que necesite ser procesada.

Con nueva data para procesar, primero se revisa que no existan datos vacios, filtran partidas no clasificatorias, se convierte el valor `gameCreation` en `fecha_local`, se calcula el kda de cada partida que ayuda bastante a ver que tambien se desempeño en una partida y se agrega como columna, se elige que columnas queden en el dataframe, se cambian el tipo de variables para eficiencia de memoria, y por ultimo se borran duplicados.

todos estos datos ya transformados se guardan en minIO para luego ser leidos y verificar que entraron nuevos datos.




> **Nota:** Recordar configurar el archivo `pipeline.conf` antes de iniciar ya que la API_Key necesita ser actualizada cada 24hs, de ser necesario estoy disponible para proveerla.
> Tambien estan los requisitos en un .txt
