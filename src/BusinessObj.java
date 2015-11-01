import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class BusinessObj implements Writable {
	private Text fullAdress= new Text();

	private Text categories=new Text();

	private Text averageRating=new Text();



	public Text getAverageRating() {
		return averageRating;
	}

	public void setAverageRating(Text averageRating) {
		this.averageRating = averageRating;
	}

	public Text getFullAdress() {
		return fullAdress;
	}

	public void setFullAdress(Text fullAdress) {
		this.fullAdress = fullAdress;
	}

	public Text getCategories() {
		return categories;
	}

	public void setCategories(Text categories) {
		this.categories = categories;
	}

	@Override
	public String toString() {
		return  fullAdress + " "+categories+" "+averageRating+" ";
	}
	
	public String toString1() {
		return  fullAdress + " "+categories;
	}
	
	public String toString2() {
		return  averageRating.toString();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		
			fullAdress.readFields(dataInput);
			categories.readFields(dataInput);
			averageRating.readFields(dataInput);
		

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {

		fullAdress.write(dataOutput);
		categories.write(dataOutput);
		averageRating.write(dataOutput);


	}


}
