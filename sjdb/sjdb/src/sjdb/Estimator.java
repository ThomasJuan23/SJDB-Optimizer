package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {

	public int total = 0;

	public Estimator() {
		// empty constructor
	}

	/*
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		op.setOutput(output);
		total += output.getTupleCount();
	}

	public void visit(Project op) {
		Relation oldrelation = op.getInput().getOutput();
		Relation newrelation = new Relation(oldrelation.getTupleCount());        //the tuple of the project is equal to the relation
		Iterator<Attribute> iter = op.getAttributes().iterator();  //get the output of the project
		while (iter.hasNext()) {
			newrelation.addAttribute(new Attribute(oldrelation.getAttribute(iter.next())));
		}
		op.setOutput(newrelation);
		total += newrelation.getTupleCount();
	}

	public void visit(Select op) {
		Predicate predicate = op.getPredicate();
		Relation oldrelation = op.getInput().getOutput();
		Attribute left = oldrelation.getAttribute(predicate.getLeftAttribute());
		Relation newrelation;
		if (predicate.equalsValue()) {  //attr=val
			newrelation = new Relation(oldrelation.getTupleCount() / left.getValueCount());  // T= T/V
			Iterator<Attribute> iter = oldrelation.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute attribute = iter.next();
				if (!attribute.equals(left)) { 
					newrelation.addAttribute(new Attribute(attribute));  //the V of other attributes does not change
				} else {  
					newrelation.addAttribute(new Attribute(left.getName(), 1)); //the V of select attribute is 1
				}
			}
		} else {  //attr1=attr2
			Attribute right = oldrelation.getAttribute(predicate.getRightAttribute());
			newrelation = new Relation(  //T=T/MAX(Vleft,Vright)
					oldrelation.getTupleCount() / Math.max(left.getValueCount(), right.getValueCount()));
			Iterator<Attribute> iter = oldrelation.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute attribute = iter.next();
				if (!attribute.equals(left) && !attribute.equals(right))   //the V of other attribtues does not change
					newrelation.addAttribute(new Attribute(attribute));
				else if (!attribute.equals(left))    //V= Min(leftV,rightV)
					newrelation.addAttribute(new Attribute(right.getName(), Math
							.min(Math.min(left.getValueCount(), right.getValueCount()), newrelation.getTupleCount())));
				else  //V= Min(leftV,rightV)
					newrelation.addAttribute(new Attribute(left.getName(), Math
							.min(Math.min(left.getValueCount(), right.getValueCount()), newrelation.getTupleCount())));
			}
		}
		op.setOutput(newrelation);
		total += newrelation.getTupleCount();
	}

	public void visit(Product op) {
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();
		Relation newrelation = new Relation(left.getTupleCount() * right.getTupleCount());  //T= tLEFT*Tright
		Iterator<Attribute> leftAttribute = left.getAttributes().iterator();
		while (leftAttribute.hasNext()) {                                 //v is the same as before
			newrelation.addAttribute(new Attribute(leftAttribute.next()));
		}
		Iterator<Attribute> rightAttribute = right.getAttributes().iterator();
		while (rightAttribute.hasNext()) {
			newrelation.addAttribute(new Attribute(rightAttribute.next()));
		}
		op.setOutput(newrelation);
		total += newrelation.getTupleCount();
	}

	public void visit(Join op) {
		Relation left = op.getLeft().getOutput();
		Relation right = op.getRight().getOutput();
		Predicate predicate = op.getPredicate();
		int leftAttribute = 0, rightAttribute = 0; //the V of right and left attribute
		List<Attribute> all = new ArrayList<>();
		all.addAll(left.getAttributes());
		all.addAll(right.getAttributes());
		Iterator<Attribute> iter = all.iterator();
		while (iter.hasNext()) {
			Attribute attribute = iter.next();
			if (attribute.equals(predicate.getLeftAttribute()))
				leftAttribute = attribute.getValueCount();  //the same as before
			if (attribute.equals(predicate.getRightAttribute()))
				rightAttribute = attribute.getValueCount();  //the same as before//
		}
		Relation newrelation = new Relation(          //T= tLeft*Tright/max(Vleft,Vright)
				left.getTupleCount() * right.getTupleCount() / Math.max(leftAttribute, rightAttribute));
		Attribute newLeft = new Attribute(predicate.getLeftAttribute().getName(),
				Math.min(Math.min(leftAttribute, rightAttribute), newrelation.getTupleCount()));  //v= min(vleft,vright)
		Attribute newRight = new Attribute(predicate.getRightAttribute().getName(),
				Math.min(Math.min(leftAttribute, rightAttribute), newrelation.getTupleCount()));
		Iterator<Attribute> iter2 = left.getAttributes().iterator();
		while (iter2.hasNext()) {
			Attribute attribute = iter2.next();
			if (!attribute.equals(newLeft))
				newrelation.addAttribute(new Attribute(attribute));  //V of other attribute is the same
			else
				newrelation.addAttribute(newLeft);
		}
		Iterator<Attribute> iter3 = right.getAttributes().iterator();
		while (iter3.hasNext()) {
			Attribute attribute = iter3.next();
			if (!attribute.equals(newRight))
				newrelation.addAttribute(new Attribute(attribute)); //V of other attribute is the same
			else
				newrelation.addAttribute(newRight);
		}
		op.setOutput(newrelation);
		total += newrelation.getTupleCount();
	}

	public int getCost(Operator plan) { //to decide the join order by comparing the total cost of the join plan
		this.total = 0;
		plan.accept(this);
		return this.total;
	}
}
