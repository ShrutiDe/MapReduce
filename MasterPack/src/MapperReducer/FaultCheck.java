package MapperReducer;
import java.util.ArrayList;
import java.util.List;

public class FaultCheck {
	int countMap;
	int countRed;
	String check;
	static List<String> mapper = new ArrayList();
	static List<String> reducer = new ArrayList();


	public FaultCheck(int countMap, int countRed) {
		super();
		this.countMap = countMap;
		this.countRed = countRed;
	}

	public static void fillMapStatus(int index, String toreturn) {
		mapper.add(index, toreturn);
	}

	public static void fillRedStatus(int index, String toreturn) {
		reducer.add(index, toreturn);
	}


	public static List<Integer> checkMapperStatus(String part) {
		List<Integer> fault = new ArrayList<>();
		if(part.equals("Mapper")) {

			for(int i = 0;i<mapper.size();i++) {
				if(!mapper.get(i).equals("Done")) {
					fault.add(i);
				}
			}
		}
		else {
			for(int i = 0;i<reducer.size();i++) {
				if(!reducer.get(i).equals("Done"))
					fault.add(i);
			}
		}
		return fault;
	}

}
