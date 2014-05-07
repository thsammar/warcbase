package org.warcbase.cwi.pig.piggybank;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class UnitDateSplit extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() != 2 || input.get(0) == null
				|| input.get(1) == null) {
			return null;
		}

		try {
			Tuple output = TupleFactory.getInstance().newTuple(1);
			String date = input.get(0).toString();
			String unit = input.get(1).toString();

			String year = date.substring(0, 4);
			String month = date.substring(4, 6);
			String day = date.substring(6, 8);

			if (unit.equals("year")) {
				output.set(0, year);
				return output.get(0).toString();
			} else if (unit.equals("month")) {
				output.set(0, month);
				return output.get(0).toString();
			} else if (unit.equals("day")) {
				output.set(0, day);
				return output.get(0).toString();
			} else
				output.set(0, date);
			return output.get(0).toString();

		} catch (Exception e) {
			return null;
		}

	}
}