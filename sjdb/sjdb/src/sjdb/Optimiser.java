package sjdb;

import java.util.*;

public class Optimiser implements PlanVisitor {
	Estimator estimator = new Estimator();
	Set<Attribute> attributes = new HashSet<>(); // set of all attributes in the plan
	Set<Predicate> predicates = new HashSet<>(); // set of all predicates in the plan
	Set<Scan> scans = new HashSet<>(); // set of all relation in the plan
	ArrayList<Attribute> top = new ArrayList<>(); // set of the attributes which project needed

	public Optimiser(Catalogue catalogue) {

	}

	@Override
	public void visit(Scan op) {
		// TODO Auto-generated method stub
		scans.add(new Scan((NamedRelation) op.getRelation()));
	}

	@Override
	public void visit(Project op) {
		// TODO Auto-generated method stub
		attributes.addAll(op.getAttributes());
		top.addAll(op.getAttributes());
	}

	@Override
	public void visit(Select op) {
		// TODO Auto-generated method stub
		predicates.add(op.getPredicate());
		attributes.add(op.getPredicate().getLeftAttribute());
		if (!op.getPredicate().equalsValue()) {
			attributes.add(op.getPredicate().getRightAttribute());
		}
	}

	@Override
	public void visit(Product op) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Join op) {
		// TODO Auto-generated method stub

	}

	public Operator optimise(Operator plan) {
		plan.accept(this);
		ArrayList<Operator> subplan = pushDown(plan); // At first, get the low level sub plans, after push the project
														// and select down
		Operator newplan = CreateAndOrderJoin(subplan, plan); // get the Permutations of all join plan, and choose the
																// cheapest one.
		return newplan;
	}

	public ArrayList<Operator> pushDown(Operator plan) { // push the project and select down
		ArrayList<Operator> subplan = new ArrayList<>(scans.size()); // the number of subplan is equal to the number of
																		// relations
		Iterator<Scan> iters = scans.iterator(); // it must use a iterators, due to the remove operation
		while (iters.hasNext()) {
			Operator operator = iters.next();
			List<Attribute> attrs = operator.getOutput().getAttributes(); // get all attributes of one relation
			Set<Attribute> usefulattrs = new HashSet<>(); // get all attributes that project get and predicate need
			Iterator<Predicate> iter2 = predicates.iterator();
			while (iter2.hasNext()) {
				Predicate predicate = iter2.next();
				if (attrs.contains(predicate.getLeftAttribute())
						&& (predicate.equalsValue() || attrs.contains(predicate.getRightAttribute()))) { 
					// the predicate is only about one relation,the select can be opereated in low level
					operator = new Select(operator, predicate); // build a select in low level, push this kind of select
																// down
					iter2.remove();
				} else {
					if (predicate.getLeftAttribute() != null) {
						usefulattrs.add(predicate.getLeftAttribute());
					}
					if (predicate.getRightAttribute() != null) {
						usefulattrs.add(predicate.getRightAttribute());
					}
				}
			}
			usefulattrs.addAll(top);
			List<Attribute> restAttrs = new ArrayList<>(usefulattrs);
			if (operator.getOutput() == null) { // avoid null output
				operator.accept(estimator);
			}
			restAttrs.retainAll(operator.getOutput().getAttributes()); // get the intersection of the needed attributes
																		// and the attributes which operator can get.
			if (restAttrs.size() > 0 && operator.getOutput().getAttributes().size() != restAttrs.size()) { 
				// the intersection  has something, and it is not equal to the scan output, because if the number of
				// intersection attribtues is equal to the attrbitues  scan  output, the extra project is useless
				Operator op = new Project(operator, restAttrs); // Only Project the useful attributes, push the project
																// down
				op.accept(estimator);
				subplan.add(op);
			} else {
				subplan.add(operator);
			}
		}
		return subplan;
	}

	public Operator CreateAndOrderJoin(ArrayList<Operator> subplan, Operator plan) { // Create and Order the Join and
																						// Product
		Operator newplan = null;
		ArrayList<Predicate> pres = new ArrayList<Predicate>(predicates);
		ArrayList<ArrayList<Predicate>> permutations = new ArrayList<ArrayList<Predicate>>();
		permutations.add(new ArrayList<Predicate>()); // start with an empty permutation
		for (Predicate element : pres) { // get all possible permutations
			ArrayList<ArrayList<Predicate>> newPermutations = new ArrayList<ArrayList<Predicate>>();
			for (ArrayList<Predicate> permutation : permutations) {
				for (int i = 0; i <= permutation.size(); i++) {
					ArrayList<Predicate> newPermutation = new ArrayList<Predicate>(permutation);
					newPermutation.add(i, element);
					newPermutations.add(newPermutation);
				}
			}
			permutations = newPermutations;
		}
		int max = Integer.MAX_VALUE; // the number which is used to get the cheapest cost
		Iterator<ArrayList<Predicate>> iters = permutations.iterator(); // create the join, or remain select
		while (iters.hasNext()) {
			Operator possibleplan = null; // a possible plan after join, which is used to replace the origin sub plan
			ArrayList<Operator> tempplan = new ArrayList<>(); // the sub plan after join or select about mulitiple
																// relation
			tempplan.addAll(subplan);
			ArrayList<Predicate> ps = iters.next();
			if (subplan.size() == 1 && subplan.get(0).getOutput() == null) {
				possibleplan = subplan.get(0);
				possibleplan.accept(estimator);
			} else {
				Iterator<Predicate> iter2 = ps.iterator();
				while (iter2.hasNext()) {
					Predicate predicate = iter2.next();
					Operator left = null, right = null;
					Iterator<Operator> subiters = tempplan.iterator();
					while (subiters.hasNext()) {
						Operator suboperator = subiters.next();
						if (suboperator.getOutput() == null) {
							suboperator.accept(estimator);
						}
						if (suboperator.getOutput().getAttributes().contains(predicate.getLeftAttribute())) { 
							// get the sub plan which the left attribue of the predicate needs
							subiters.remove();
							left = suboperator;
						} else if (suboperator.getOutput().getAttributes().contains(predicate.getRightAttribute())) { 
							// get the sub plan which the right attribue of the predicate ddneeds
							subiters.remove();
							right = suboperator;
						}
					}
					if (left != null && right == null) { // according to if null of left and right, to create the join
															// or remain select
						possibleplan = new Select(left, predicate);
					} else if (left == null && right != null) {
						possibleplan = new Select(right, predicate);
					} else if (left != null && right != null) {
						possibleplan = new Join(left, right, predicate);
					}
					if (left != null || right != null) {
						iter2.remove(); // finish the sub plan
					}
					if (possibleplan.getOutput() == null) {
						possibleplan.accept(estimator);
					}
					ArrayList<Attribute> need = new ArrayList<>(); // get all needed attributes
					need.addAll(top);
					Iterator<Predicate> iter3 = ps.iterator();
					while (iter3.hasNext()) {
						Predicate pd = iter3.next();
						if (pd.getLeftAttribute() != null) {
							need.add(pd.getLeftAttribute());
						}
						if (predicate.getRightAttribute() != null) {
							need.add(pd.getRightAttribute());
						}
					}
					List<Attribute> as = new ArrayList<>();
					as.addAll(possibleplan.getOutput().getAttributes());
					as.retainAll(need);
					if (possibleplan.getOutput().getAttributes().size() != as.size()) { // only project the useful
																						// attributes
						possibleplan = new Project(possibleplan, as);

					}
					tempplan.add(possibleplan); // replace the origin sub plan
				}

			}
			possibleplan = tempplan.get(0); // get the whole plan
			int costEstimate = estimator.getCost(possibleplan); // get the cost
			// System.out.println(possibleplan.toString() + costEstimate);
			if (costEstimate < max) { // get the cheapest order
				newplan = possibleplan;
				max = costEstimate;
			}
		}

		return newplan;
	}

}
