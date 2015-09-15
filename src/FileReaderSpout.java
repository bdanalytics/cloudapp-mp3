
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private String inpFileName;
  private FileReader fileReader;
  private boolean completed = false;

  FileReaderSpout(String argFileName) {
	this.inpFileName = new String(argFileName);
  }

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

    this.context = context;

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */
    //String inpFileName = new String("data.txt");

    try {
        this.fileReader = new FileReader(this.inpFileName);
    } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file " + inpFileName);
    }

    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */

        if (this.completed) {
            Utils.sleep(100);
        }

        String sentence;
        BufferedReader reader = new BufferedReader(this.fileReader);
        try {
            while ((sentence = reader.readLine().trim()) != null) {
                this._collector.emit(new Values(sentence));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
	try {
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
