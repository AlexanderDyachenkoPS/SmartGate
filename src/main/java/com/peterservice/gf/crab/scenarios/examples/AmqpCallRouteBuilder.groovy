package com.peterservice.gf.crab.scenarios.examples

import com.peterservice.crab.zooservice.ZooService
import com.peterservice.crab.zooservice.ZooServiceBuilder
import com.rabbitmq.client.ConnectionFactory
import org.apache.camel.CamelContext
import org.apache.camel.Exchange
import org.apache.camel.Expression
import org.apache.camel.LoggingLevel
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.impl.CompositeRegistry
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.impl.SimpleRegistry
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class AmqpCallRouteBuilder extends RouteBuilder{

    public static final String AMQP_CALL_ROUTE_NAME = "amqpCall"
    //in headers
    public static final String amqpVirtualHostKey = "amqpVirtualHost"
    public static final String amqpExchangeKey = "amqpExchange"
    public static final String amqpRoutingKey = "amqpRoutingKey"
    public static final String amqpShardingValueKey = "amqpShardingValue"
    public static final String amqpShardingCountKey = "amqpShardingCount"

    @Override
    void configure() throws Exception {
         String zookeeperConnect = System.getProperty("zoo.server");
       // ObjectMapper mapper = ScenariosHelper.buildObjectMapper()
        Logger logger = LoggerFactory.getLogger(AmqpCallRouteBuilder.class)



        Map amqpOptions = [:]
        try {
            ZooService zooService = ZooServiceBuilder.builder().withConnectionString(zookeeperConnect).usingNameSpace("ps").build();
            //ZooService encryptedZooService = ScenariosHelper.getEncryptedZooService()
            amqpOptions = [
                    rabbitPort :     zooService.getInt("/config/apps/crab-scenarios/common-internal/rabbitPort"),
                    rabbitHost :     zooService.getString("/config/apps/crab-scenarios/common-internal/rabbitHost"),
                    rabbitUser :     zooService.getString("/config/apps/crab-scenarios/common-internal/rabbitUser"),
                    rabbitPassword : zooService.getString("/config/apps/crab-scenarios/common-internal/rabbitPassword")
            ]
        } catch (Exception ex) {
            logger.error("Exception while getting amqp settings from Zookeeper: {}", ex)
        }

        ConnectionFactory connectionFactory = new ConnectionFactory()
        connectionFactory.setUsername(amqpOptions["rabbitUser"] as String)
        connectionFactory.setPassword(amqpOptions["rabbitPassword"] as String)
        connectionFactory.setPort(amqpOptions["rabbitPort"] as Integer)
        connectionFactory.setHost(amqpOptions["rabbitHost"] as String)
        connectionFactory.setHandshakeTimeout(20000)
        connectionFactory.setAutomaticRecoveryEnabled(true)

        CamelContext camelContext = getContext()
        SimpleRegistry simpleRegistry = new SimpleRegistry()
        simpleRegistry.put("amqpConnectionFactory", connectionFactory)

        CompositeRegistry compositeRegistry = new CompositeRegistry()
        compositeRegistry.addRegistry(camelContext.getRegistry())
        compositeRegistry.addRegistry(simpleRegistry)
        ((DefaultCamelContext) camelContext).setRegistry(compositeRegistry)

        /**
         * Отправка AMQP-сообщения.
         *
         * Headers:
         *  amqpVirtualHost -  Rabbit virtual host - optional - String
         *      Если не задано, то используется virtual host "/"
         *  amqpExchange - Имя exchange, куда будет отправлено сообщение - mandatory - String
         *  amqpRoutingKey - Имя routing key для сообщения - mandatory - String
         *  amqpShardingValue - Значение, по которому будет высчитан номер шарда - nullable - String
         *      Если не задано, то попытка раскрыть макроподстановку в amqpRoutingKey НЕ выполняется
         *  amqpShardingCount - Максимальное количество шардов - nullable - String
         *      Если не задано amqpShardingValue, то параметр игнорируется
         *
         * Properties:
         *  -
         *
         * Body:
         *  Тело сообщения.
         * Out headers:
         *  -
         */

        from("direct:AMQP_CALL_ROUTE_NAME").id("AMQP_CALL_ROUTE_NAME")
                .log(LoggingLevel.DEBUG, logger, "Send amqp message started. Headers: \${headers} Body: \${body}")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                String virtualHost = exchange.in.getHeader(amqpVirtualHostKey, String.class)
                if (virtualHost == null || virtualHost.isEmpty()) virtualHost = "/"
                String exchangeName = exchange.in.getHeader(amqpExchangeKey, String.class)
                String routingKey = exchange.in.getHeader(amqpRoutingKey, String.class)
                Long shardingValue = exchange.in.getHeader(amqpShardingValueKey, Long.class)
                Integer shardingCount = exchange.in.getHeader(amqpShardingCountKey, Integer.class)
                if (shardingValue != null) {
                    int shardNumber = shardingValue % shardingCount + 1
                    routingKey = String.format(routingKey, shardNumber)
                }

                connectionFactory.setVirtualHost(virtualHost)

                String rabbitUrl = String.format(
                        "rabbitmq://%s:%s/%s?username=%s&password=%s&vhost=%s&routingKey=%s&declare=false&connectionFactory=#amqpConnectionFactory",
                        amqpOptions["rabbitHost"], amqpOptions["rabbitPort"], exchangeName, amqpOptions["rabbitUser"], amqpOptions["rabbitPassword"], virtualHost, routingKey
                )
                logger.debug("Url for send amqp message - {}", rabbitUrl)
                logger.debug("Body for send amqp message - {}", exchange.in.body)
                exchange.setProperty("rabbitMqUrl", rabbitUrl)
            }
        })
                .removeHeaders("*")
                .choice()
                .when(simple("\${exchangeProperty.createOrReopen} != null"))
                .setHeader("method", constant("CREATE_OR_REOPEN"))
                .endChoice()
                .end()
                .choice()
                .when(simple("\${exchangeProperty.ps.crab.context.pstxid} != null"))
                .setHeader("pstxid", simple("\${exchangeProperty.ps.crab.context.pstxid}"))
                .endChoice()
                .end()
      /*          .setHeader("eventDate", ({
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ").print(DateTime.now(DateTimeZone.forTimeZone(mapper.getDateFormat().getTimeZone())))
        } as Expression))
     */           .setHeader("sender", constant("crab-scenarios"))
                .recipientList(simple("\${exchangeProperty.rabbitMqUrl}"))
                .log(LoggingLevel.DEBUG, logger, "Amqp message has sent")
    }
}
