package com.peterservice.gf.crab.scenarios.examples

import com.peterservice.crab.zooservice.ZooService
import com.peterservice.crab.zooservice.ZooServiceBuilder
import com.rabbitmq.client.ConnectionFactory
import org.apache.camel.Exchange
import org.apache.camel.LoggingLevel
import org.apache.camel.Message
import org.apache.camel.Processor
import org.apache.camel.builder.RouteBuilder
import org.json.simple.JSONObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory


class SmartGateOperationExamples extends RouteBuilder {

    static String zookeeperConnect = System.getProperty("zoo.server");

    Logger logger = LoggerFactory.getLogger(SlowPokeRouteBuilder.class)

    @Override
    def void configure() throws Exception {

        ZooService zoo = ZooServiceBuilder.builder().withConnectionString(zookeeperConnect).usingNameSpace("ps").build();


/*
    Работа с входными параметрами
    В каждой точке нам доступен объект Exchange
    Это и есть наш обменник, при помощи которого можно манипулировать данными в процессе выполнения сценария

    А еще этот объект, а точнее его определенные заголовки являются API, позволяющим взаимодействовать с движком CRAB
    Достигается это при помощи выставления залоговком с определенным именем.

    Кроме того, этот объект содержит ссылку(?) на объект Message (exchange.in) - это и есть сообщение, которое идет по маршруту.

    Итого имеем вот такие конвенции

        ps.crab.context.{key}   - сохраняемые заголовки
        ps.crab.system.{key}    - системные read-only заголовки
        ps.crab.{key}           - crab заголовки, которые устанавливает разработчик скрипта
        ps.crab.script.{key}    - пользовательские заголовки

    Теперь подробности - собственно то "API", про которое говорилось выше - манипулируя залоговками можно договориться с CRAB

             ps.crab.result.value - результат исполнения скрипта, в случае запроса нотификации, будет отправлен вызывающей стороне
             ps.crab.result.description - дополнительное описание результата, отображается в GUI
             ps.crab.result.status - статус исполнения заявки, в случае успешного (дефолтного) исполнения = 0, в случае ошибки формируется значение отличное от "0"




*/

/*
    Ловим на входе
        message4Value
        message4Descritpion
    после чего сохраняем их в
        ps.crab.result.value
        ps.crab.result.description
    соответственно
      завершаемся успехом
*/
        from("crab:workWithInput").id("workWithInput") // Сюда мы попадем, если сформируем требование с operation=workWithInput
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;
                inMessage.setHeader("ps.crab.result.value", inMessage.getHeader('message4Value', String.class))
                inMessage.setHeader("ps.crab.result.description", inMessage.getHeader('message4Descritpion', String.class));
            }
        })

        /*
           Сценарии должны завершаться либо успехом, либо фейлом - т.е. сценарист должен использовать либо

           .to("crab:success")
           либо
           .to("crab:fail")

            По умолчанию краб будет выставлять crab:fail

        */
                .to("crab:success")

        from("crab:workWithProperties").id("workWithProperties") // Сюда мы попадем, если сформируем требование с operation=workWithproperties
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;

                Map savedProperties = new HashMap<String,Object>()
                exchange.getProperties().each { k, v ->
                    savedProperties.put(k,v)
                }
                logger.debug(savedProperties.toString());

                inMessage.setHeader("ps.crab.result.value", savedProperties.toString());
                inMessage.setHeader("ps.crab.result.description", savedProperties.toString());

                inMessage.setHeader("ps.crab.context.AAA", savedProperties.toString());
                inMessage.setHeader("ps.crab.BBB", savedProperties.toString());
                exchange.setProperty("ps.crab.CCC",inMessage.getHeader('message4Value', String.class));
                exchange.setProperty("ps.crab.context.DDD",inMessage.getHeader('message4Value', String.class));
            }
        })

                .to("crab:workWithProperties2Wait")


                .to("crab:success")

                from("crab:workWithPropertiesWait").id("workWithPropertiesWait") // Сюда мы попадем, если сформируем требование с operation=workWithPropertiesWait
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;

                Map savedProperties = new HashMap<String,Object>()
                exchange.getProperties().each { k, v ->
                     savedProperties.put(k,v)
                }
                logger.debug(savedProperties.toString());

                inMessage.setHeader("ps.crab.context.AAA", savedProperties.toString());
                inMessage.setHeader("ps.crab.BBB", savedProperties.toString());
                exchange.setProperty("ps.crab.CCC",inMessage.getHeader('message4Value', String.class));
                exchange.setProperty("ps.crab.context.DDD",inMessage.getHeader('message4Value', String.class));
            }
        })

        /*
           Сценарии должны завершаться либо успехом, либо фейлом - т.е. сценарист должен использовать либо

           .to("crab:success")
           либо
           .to("crab:fail")

            По умолчанию краб будет выставлять crab:fail

        */

                        .to("crab:workWithProperties2Wait?waitTime=10")

        from("crab:workWithProperties2Wait").id("workWithProperties2Wait")
                .process(new Processor() {
            @Override
            void process(Exchange exchange) throws Exception {
                Message inMessage = exchange.in;

                Map savedProperties = new HashMap<String,Object>()
                exchange.getProperties().each { k, v ->
                    savedProperties.put(k,v)
                }
                logger.debug(savedProperties.toString());

                Map order = [
                        DDD: exchange.getProperty('ps.crab.context.DDD'),
                        BBB: inMessage.getHeader("ps.crab.BBB"),
                        CCC: exchange.getProperty('ps.crab.CCC'),
                        AAA: inMessage.getHeader("ps.crab.context.AAA")
                ];

                JSONObject jo = new  JSONObject(order);

                inMessage.setHeader("ps.crab.result.value", jo);
                inMessage.setHeader("ps.crab.result.description", jo.toString());

            }
        })

                .to("crab:success")
}
}
