package com.peterservice.gf.crab.scenarios.examples

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.ISO8601DateFormat
import com.peterservice.crab.zooservice.ZooService
import com.peterservice.crab.zooservice.ZooServiceBuilder
import com.rabbitmq.client.ConnectionFactory
import org.apache.camel.Exchange
import org.apache.camel.LoggingLevel
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.builder.RouteBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import com.peterservice.crab.response.model.orders.CallbackRm
import static com.peterservice.gf.crab.scenarios.examples.AmqpCallRouteBuilder.AMQP_CALL_ROUTE_NAME


class SlowPokeRouteBuilder extends RouteBuilder {
    static String zookeeperConnect = System.getProperty("zoo.server");

    Logger logger = LoggerFactory.getLogger(SlowPokeRouteBuilder.class)


    @Override
    def void configure() throws Exception {

        ZooService zoo = ZooServiceBuilder.builder().withConnectionString(zookeeperConnect).usingNameSpace("ps").build();

        PlayerOptions playerOptions         = new PlayerOptions();
        playerOptions.emuHost               = zoo.getString("/config/apps/crab-scenarios/slowpoke/emuHost");
        playerOptions.emuPort               = zoo.getString("/config/apps/crab-scenarios/slowpoke/emuPort");
        playerOptions.emuUrl                = zoo.getString("/config/apps/crab-scenarios/slowpoke/emuUrl");
        playerOptions.amqpCallback          = zoo.getString("/config/apps/crab-scenarios/common-internal/amqpCallback")
        playerOptions.rabbitVhost           = zoo.getString("/config/apps/crab-scenarios/common-internal/rabbitVhost");
        playerOptions.rabbitExchange        = zoo.getString("/config/apps/crab-scenarios/slowpoke/rabbitExchange");
        playerOptions.routingKeyNewOrder    = zoo.getString("/config/apps/crab-scenarios/slowpoke/routingKey");
        playerOptions.postponeTime          = zoo.getString("/config/apps/crab-scenarios/slowpoke/postponeTime");
        playerOptions.wait4Event            = zoo.getString("/config/apps/crab-scenarios/slowpoke/wait4Event");
/*
    Просто сценарий, принимающий на вход время, которое надо поспать - переменная ps.crab.context.waittime
    В ответ пишет LEEEEEEEEEEEEEEROY!!!!, то же самое пишет в лог
    Завершается всегда успехом
*/
        from("crab:LeeroyJenkins").id("LeeroyJenkinsRoute")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;
                exchange.setProperty("ps.crab.context.waitTime", inMessage.getHeader('waitTime', String.class));
                exchange.setProperty("ps.crab.context.message", inMessage.getHeader('message', String.class));
                inMessage.setHeader("ps.crab.result.value","LEEEEEEEEEEEEEEROY!!!!\n" + "Your message is : " +  inMessage.getHeader('message', String.class))

            }
        })
                .log(LoggingLevel.DEBUG, logger, "LEEEEEEEEEEEEEEROY!!!!")
                .delay(exchangeProperty("ps.crab.context.waitTime"))

                .to("crab:success")

/*
    Сценарий, который зовется асинхронно - он должен запуститься и отослать callback
*/
        from("crab:LeeroyJenkinsAsync").id("LeeroyJenkinsAsync")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;
                exchange.setProperty("ps.crab.context.waitTime", inMessage.getHeader('waitTime', String.class));
                JSONObject jo = new JSONObject();
                jo.put("msg","!!!! LEEEEEEEEEEEEEEROY Async !!!!");
                inMessage.setHeader("ps.crab.result.value", jo);
            }
        })
                .log(LoggingLevel.DEBUG, logger, "!!!! LEEEEEEEEEEEEEEROY Async !!!!")

                .delay(exchangeProperty("ps.crab.context.waitTime"))
                .to("crab:success")
/*
    Сценарий, принимающий на вход IMSI, который передается на эмулятор - переменная ps.crab.context.IMSI
*/
        from("crab:askEmul").id("askEmul")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;

                String connectString = "http://" + playerOptions.emuHost + ":" + playerOptions.emuPort + "/";
                String fullUrl = connectString + playerOptions.emuUrl;

                exchange.setProperty("ps.crab.context.IMSI", inMessage.getHeader('IMSI', String.class));
                inMessage.setHeader(Exchange.HTTP_URI, fullUrl);
                inMessage.setHeader(Exchange.HTTP_METHOD, "GET");
                inMessage.setHeader(Exchange.CONTENT_TYPE, 'application/xml');
            }
        })
                .to("http4://hexhost?throwExceptionOnFailure=false")
                .setHeader("ps.crab.result.description", simple("HEX response code \${headers.CamelHttpResponseCode} and body: \${body}"))
                .log(LoggingLevel.DEBUG, logger, "askEmul")
                .choice()
                .when(xpath("//VARIABLES/RESULT=1"))
                    .setHeader("ps.crab.result.value", simple("1"))
                    .to("crab:success")
                .endChoice()
                .otherwise()
                    .setHeader("ps.crab.result.value", simple("0"))
                    .to("crab:fail")
                .endChoice()
/*
   Спим. Без workflow засыпаем навсегда
*/
        from("crab:operationWithSleep").id("operationWithSleep")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;

                inMessage.setHeader("ps.crab.sleep.until", "2018-10-04T06:00:00.000+00:00");

                inMessage.setHeader("ps.crab.result.value","LEEEEEEEEEEEEEEROY!!!!")

            }
        })
                .to("crab:sleep")
                .to("crab:success")
/*
   Плюем в Amqp и встаем на ожидании callback
*/
        from("crab:putAMQP").id("putAMQP")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;
                exchange.setProperty("ps.crab.context.correlationId", UUID.randomUUID().toString());
                String correlationId = exchange.getProperty("ps.crab.context.correlationId", String.class);
                List orders = new ArrayList();
                Map order = [
                        externalId: correlationId,
                        processingType: "parallel",
                        operation: [
                                name: "LeeroyJenkinsAsync",
                                params: [
                                        [
                                        name: "waitTime",
                                        value: inMessage.getHeader('waitTime', String.class)
                                ]
                                ]
                        ],
                        metadata:[
                                callbackCorrelationId: correlationId,
                                callbackUrl: playerOptions.amqpCallback,
                                callbackTimeZone: ""
                        ]
                ];
                orders.add(order);
                JSONObject jo = new  JSONObject(order);
                JSONArray ja = new JSONArray ();
                ja.add(jo);
                inMessage.setBody(ja.toString());
                //inMessage.setBody("aaaa");
                exchange.setProperty("createOrReopen", true)

            }
        })

                .setHeader("amqpVirtualHost", constant(playerOptions.rabbitVhost))
                .setHeader("amqpExchange", constant(playerOptions.rabbitExchange))
                .setHeader("amqpRoutingKey", constant(playerOptions.routingKeyNewOrder))
                .to("direct:AMQP_CALL_ROUTE_NAME")
                .setBody(constant(null))
                .setHeader("ps.crab.result.description", constant("Send CCM detail AMQP message"))
                .setHeader("ps.crab.correlationId", exchangeProperty("ps.crab.context.correlationId"))
                .recipientList().simple("crab:catchCallback?waitForEvent=*&waitTime="+playerOptions.postponeTime)
                .log(LoggingLevel.DEBUG, logger, "!!!LAST LOG!!!")
                .to("crab:success")


/*
  Обработка callback
*/

        from("crab:catchCallback").id("catchCallback")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;
                List<CallbackRm> callbacks = (List<CallbackRm>) exchange.getProperty('ps.crab.system.callbacks');

                inMessage.setHeader("ps.crab.result.value",callbacks.toString())

            }
        })
                .to("crab:success")

    }



    static class PlayerOptions {
        String emuHost;
        String emuPort;
        String emuUrl;
        String amqpCallback;
        String rabbitVhost;
        String rabbitExchange;
        String routingKeyNewOrder;
        String postponeTime;
        String wait4Event;
    }
}


