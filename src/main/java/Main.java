import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;

public class Main {

    private static Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String []args) throws Exception{

        // Factory for the event
        MessageEventFactory factory = new MessageEventFactory();

        //Specify ring buffer size, must power of 2
        int bufferSize = 64;

        //Construct Disruptor
        Disruptor<MessageEvent> disruptor = new Disruptor<>(factory, bufferSize, DaemonThreadFactory.INSTANCE);

        //Connect the Event Handler
        disruptor.handleEventsWith(new MessageEventHandler());

        //Start the disruptor, starts all threads running
        disruptor.start();

        //Get the ring buffer from the Disruptor to be used for publishing
        RingBuffer<MessageEvent> ringBuffer = disruptor.getRingBuffer();

        MessageEventPublisher producer = new MessageEventPublisher(ringBuffer);

        // Publish data to ring buffer
        ByteBuffer bb = ByteBuffer.allocate(8);
        for(int i=0; i<24; i++){
            bb.putLong(0,i);
            producer.onData(bb);
            LOG.info("Publishing data to the Ring Buffer: [{}]", bb);
        }
    }

    //Event Message
    public static class MessageEvent {
        private String ArbMessage;
        public void set(String ArbMessage){
            this.ArbMessage = ArbMessage;
            LOG.info("Message Event - Arbitrary Message: [{}]", ArbMessage);
        }
    }


    //Event Constructor
    public static class MessageEventFactory implements EventFactory<MessageEvent>
    {
        public MessageEvent newInstance()
        {
            MessageEvent someEvent = new MessageEvent();
            LOG.info("MessageEvenFactory - MessageEvent: [{}]", String.format(someEvent.toString()));
            return someEvent;
        }
    }


    //Event Handler
    public static class MessageEventHandler implements EventHandler<MessageEvent>
    {
        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch)
        {
            LOG.info("MessageEventHandler - MessageEvent: [{}]", String.format(event.toString()));
        }
    }



    //Publish Using Translator
    public class MessageEventPublisherWithTranslator {
        private final EventTranslatorOneArg<MessageEvent, ? super ByteBuffer> TRANSLATOR = new EventTranslatorOneArg<MessageEvent, ByteBuffer>() {
            @Override
            public void translateTo(MessageEvent messageEvent, long l, ByteBuffer byteBuffer) {
                messageEvent.set(String.valueOf(byteBuffer.getLong(0)));
                LOG.info("MessageEventPublisherWithTranslator - EventTranslatorOneArg - MessageEvent: [{}]", String.format(messageEvent.toString()));
            }
        };
        private final RingBuffer<MessageEvent> ringBuffer;

        public MessageEventPublisherWithTranslator(RingBuffer<MessageEvent> ringBuffer) {
            this.ringBuffer = ringBuffer;
            LOG.info("MessageEventPublisherWithTranslator - MessageEventPublisherWithTranslator: [{}]", String.format(ringBuffer.toString()));
        }

        public void onData(ByteBuffer byteBuffer)
        {
            ringBuffer.publishEvent(TRANSLATOR, byteBuffer);
            LOG.info("MessageEventPublisherWithTranslator - onData - publishEvent: [{}]", String.format(ringBuffer.toString()));
        }
    }



    //Publish Using the Legacy API
    public static class MessageEventPublisher{
        private final RingBuffer<MessageEvent> ringBuffer;

        public MessageEventPublisher(RingBuffer<MessageEvent> ringBuffer){
            this.ringBuffer = ringBuffer;
            LOG.info("MessageEventPublisher - MessageEventProducer - ringBuffer: [{}]", String.format(ringBuffer.toString()));
        }

        public void onData(ByteBuffer byteBuffer){
            //Grab next sequence
            long sequence = ringBuffer.next();
            LOG.info("MessageEventPublisher - onData - sequence: [{}]", sequence);

            try
            {
                //Get the entry in the Disruptor for the sequuence
                MessageEvent event = ringBuffer.get(sequence);

                //Fill with data
                event.set(String.valueOf(byteBuffer.getLong(0)));
                LOG.info("MessageEventPublisher - onData - event: [{}]", String.format(event.toString()));
            }
            finally {
                ringBuffer.publish(sequence);
            }
        }
    }
};
