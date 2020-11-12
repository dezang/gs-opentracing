const {Kafka} = require('kafkajs');
const {initTracer, initTracerFromEnv } = require('jaeger-client')
const { FORMAT_TEXT_MAP, Tags } = require('opentracing')
const IS_DEBUG = false

/*
 *  Init Kafka
 */
const kafka = new Kafka({
  clientId: 'app-consumer-nodejs',
  brokers: ['localhost:9092']
})

const consumer = kafka.consumer({groupId: 'test-nodejs-group'})

/*
  Init Tracer
 */
let config = {
  serviceName: 'app-consumer-nodejs',
  sampler: {
    type: "const",
    param: 1,
  },
  reporter: {
    logSpan: true,
  }
};
let options = {
  logger: {
    info: function logInfo(msg) {
      console.log("INFO ", msg);
    },
    error: function logError(msg) {
      console.log("ERROR", msg);
    },
  },
};

let tracer = IS_DEBUG ? initTracer(config, options) : initTracerFromEnv({ serviceName: 'app-consumer-nodejs'}, {})
console.log(tracer)

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({topic: 'test.tracing', fromBeginning: true})

  await consumer.run({
    eachMessage: async ({topic, partition, message}) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
        headers: message.headers,
        trace: message.headers['uber-trace-id'].toString()
      })

      /*
        Make Span
       */
      const parentSpanContext = tracer.extract(FORMAT_TEXT_MAP, {
        "uber-trace-id": message.headers["uber-trace-id"].toString()
      })
      console.log(parentSpanContext, parentSpanContext)
      const span = tracer.startSpan("consume", {
        childOf: parentSpanContext,
        tags: {[Tags.SPAN_KIND]: Tags.SPAN_KIND_MESSAGING_CONSUMER}
      });
      const ctx = { span }

      /*
        Working
       */
      await work(ctx)
      await work(ctx)
      await work(ctx)

      /*
        Finish Span
       */
      span.finish()
      console.log('finish')
    }
  })
}

const work = async (ctx) => {
  ctx = {
    span: tracer.startSpan("working", { childOf: ctx.span })
  }
  return new Promise(resolve => {
    setTimeout(() => {
      console.log("working...")
      ctx.span.finish()
      resolve()
    }, 500)
  })
}

run().catch(console.error)
