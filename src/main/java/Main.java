import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

    private static Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String []args) throws Exception{

        //Specify ring buffer size, must power of 2
        int bufferSize = 128;

        //Construct Disruptor
        Disruptor<Message> disruptor = new Disruptor<>(Message.EVENT_FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);

        //Connect the Event Handler
        disruptor.handleEventsWith(new SequentialWorkHandler());

        //Start the disruptor, starts all threads running
        disruptor.start();

        //Get the ring buffer from the Disruptor to be used for publishing
        RingBuffer<Message> ringBuffer = disruptor.getRingBuffer();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // Loop a bunch of times to simulate a stream of events coming in.
        for (int eventCount = 0; eventCount < 256; eventCount++) {

            long nextSequence = ringBuffer.next();

            Message message = ringBuffer.get(nextSequence);
            message.setMessage("Message from a firewall.");
            ringBuffer.publish(nextSequence);

            LOG.info("Publishing eventCount [{}] sequence [{}]", eventCount, nextSequence);
        }

        // Wait to exit the main thread until buffer size is zero.
        while (true) {
            Thread.sleep(500);
            if (ringBuffer.remainingCapacity() != bufferSize) {
            } else {
                // Log final processing time and Terminate the main thread when done processing.
                LOG.info("** Elapsed time [{}] **", stopWatch.getTime());
                break;
            }
        }
    }
};
