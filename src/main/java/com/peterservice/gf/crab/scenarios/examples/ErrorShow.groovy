package com.peterservice.gf.crab.scenarios.examples

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


class ErrorShow extends RouteBuilder {

    static String zookeeperConnect = System.getProperty("zoo.server");

    Logger logger = LoggerFactory.getLogger(ErrorShow.class)

    @Override
    def void configure() throws Exception {

        PlayerOptions playerOptions         = new PlayerOptions();
        ZooService zooService = ZooServiceBuilder.builder().withConnectionString(zookeeperConnect).usingNameSpace("ps").build();
        playerOptions.emuHost               = zooService.getString("/config/apps/crab-scenarios/slowpoke/emuHost");
        playerOptions.emuPort               = zooService.getString("/config/apps/crab-scenarios/slowpoke/emuPort");
        playerOptions.emuUrl                = zooService.getString("/config/apps/crab-scenarios/slowpoke/emuUrl");
        playerOptions.aaa                   = zooService.getInt("/config/apps/crab-scenarios/aaaa");

         from("crab:error0001").id("error0001")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;

                Map savedProperties = new HashMap<String,Object>()
                exchange.getProperties().each { k, v ->
                    savedProperties.put(k,v)
                }


                inMessage.setHeader("ps.crab.result.value", savedProperties);
                inMessage.setHeader("ps.crab.result.description", savedProperties);

            }
        })
                 .to("crab:success")

        from("crab:error0002").id("error0002")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;

                String bd = inMessage.getBody();

                inMessage.setHeader("ps.crab.result.value", bd);
                inMessage.setHeader("ps.crab.result.description", bd);

            }
        })

        from("crab:error0003").id("error0003")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;

                String connectString = "http://" + playerOptions.emuHost + ":" + playerOptions.emuPort + "/";
                String fullUrl = connectString + playerOptions.emuUrl;

                exchange.setProperty("ps.crab.context.IMSI", inMessage.getHeader('IMSI', String.class));
              //  inMessage.setHeader(Exchange.HTTP_URI, fullUrl);
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
/*                .process(new Processor() {
                     @Override
                         void process(Exchange exchange) throws Exception {
                          exchange.getIn().setBody(null);
                        }
                    })
*/                .to("crab:error0003AfterWait?waitTime=5")
                .endChoice()
                .otherwise()
                .setHeader("ps.crab.result.value", simple("0"))
                .process(new Processor() {
                    @Override
                        void process(Exchange exchange) throws Exception {
                                exchange.getIn().setBody(null);
                            }
                 })
                .to("crab:error0003AfterWait?waitTime=5")
                .endChoice()

        from("crab:error0003AfterWait").id("error0003AfterWait")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;



                String bd = inMessage.getBody();

                inMessage.setHeader("ps.crab.result.value", bd);
                inMessage.setHeader("ps.crab.result.description", bd);

            }
        })
                .to("crab:success")
    }
    static class PlayerOptions {
        String emuHost;
        String emuPort;
        String emuUrl;
        Integer aaa;
    }
}
