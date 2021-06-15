package mypackage;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class WordCount {
    /**
     * This is the POJO (Plain Old Java Object) that is being used for all the operations. As long
     * as all fields are public or have a getter/setter, the system can handle them
     */
    public static class Word {

        // fields
        private String word;
        private int frequency;

        // constructors
        public Word() {}

        public Word(String word, int i) {
            this.word = word;
            this.frequency = i;
        }

        // getters setters
        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "Word=" + word + " freq=" + frequency;
        }
    }

//    public static void main(String[] args) throws Exception {
//
//        final ParameterTool params = ParameterTool.fromArgs(args);
//
//        // set up the execution environment
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        // make parameters available in the web interface
//        env.getConfig().setGlobalJobParameters(params);
//
//        // get input data
//        DataSet<String> text;
//        if (params.has("input")) {
//            // read the text file from given input path
//            text = env.readTextFile(params.get("input"));
//        } else {
//            // get default test text data
//            System.out.println("Executing WordCount example with default input data set.");
//            System.out.println("Use --input to specify file input.");
//            text = WordCountData.getDefaultTextLineDataSet(env);
//        }
//
//        DataSet<Word> counts =
//                // split up the lines into Word objects (with frequency = 1)
//                text.flatMap(new Tokenizer())
//                        // group by the field word and sum up the frequency
//                        .groupBy("word")
//                        .reduce(
//                                new ReduceFunction<Word>() {
//                                    @Override
//                                    public Word reduce(Word value1, Word value2) throws Exception {
//                                        return new Word(
//                                                value1.word, value1.frequency + value2.frequency);
//                                    }
//                                });
//
//        if (params.has("output")) {
//            counts.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
//            // execute program
//            env.execute("WordCount-Pojo Example");
//        } else {
//            System.out.println("Printing result to stdout. Use --output to specify output path.");
//            counts.print();
//        }
//    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple Word objects.
     */
    public static final class Tokenizer implements FlatMapFunction<String, Word> {

        @Override
        public void flatMap(String value, Collector<Word> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Word(token, 1));
                }
            }
        }
    }

    public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
        String[] WORDS =
                new String[] {
                        "To be, or not to be,--that is the question:--",
                        "Whether 'tis nobler in the mind to suffer",
                        "The slings and arrows of outrageous fortune",
                        "Or to take arms against a sea of troubles,",
                        "And by opposing end them?--To die,--to sleep,--",
                        "No more; and by a sleep to say we end",
                        "The heartache, and the thousand natural shocks",
                        "That flesh is heir to,--'tis a consummation",
                        "Devoutly to be wish'd. To die,--to sleep;--",
                        "To sleep! perchance to dream:--ay, there's the rub;",
                        "For in that sleep of death what dreams may come,",
                        "When we have shuffled off this mortal coil,",
                        "Must give us pause: there's the respect",
                        "That makes calamity of so long life;",
                        "For who would bear the whips and scorns of time,",
                        "The oppressor's wrong, the proud man's contumely,",
                        "The pangs of despis'd love, the law's delay,",
                        "The insolence of office, and the spurns",
                        "That patient merit of the unworthy takes,",
                        "When he himself might his quietus make",
                        "With a bare bodkin? who would these fardels bear,",
                        "To grunt and sweat under a weary life,",
                        "But that the dread of something after death,--",
                        "The undiscover'd country, from whose bourn",
                        "No traveller returns,--puzzles the will,",
                        "And makes us rather bear those ills we have",
                        "Than fly to others that we know not of?",
                        "Thus conscience does make cowards of us all;",
                        "And thus the native hue of resolution",
                        "Is sicklied o'er with the pale cast of thought;",
                        "And enterprises of great pith and moment,",
                        "With this regard, their currents turn awry,",
                        "And lose the name of action.--Soft you now!",
                        "The fair Ophelia!--Nymph, in thy orisons",
                        "Be all my sins remember'd."
            };
        return env.fromElements(WORDS);
    }
}
