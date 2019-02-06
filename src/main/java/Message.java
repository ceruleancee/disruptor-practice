import com.lmax.disruptor.EventFactory;

/**
 * The object which is being pushed to the ring buffer.
 */
class Message {

    private String message;

    final static EventFactory EVENT_FACTORY = new EventFactory() {
        @Override
        public Object newInstance() {
            return new Message();
        }
    };

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}