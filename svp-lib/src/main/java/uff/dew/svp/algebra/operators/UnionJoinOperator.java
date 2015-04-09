package uff.dew.svp.algebra.operators;

import uff.dew.svp.algebra.basic.PatternTree;
import uff.dew.svp.algebra.basic.TreeNode;
import uff.dew.svp.algebra.basic.TreeNode.RelationTypeEnum;
import uff.dew.svp.exceptions.AlgebraParserException;

public class UnionJoinOperator extends AbstractOperator {

	public UnionJoinOperator() {
		super();
	}
	
	public void generateApt() throws AlgebraParserException{
		if (this.hasChild()){
			this.apt = new PatternTree(this);
			TreeNode n = new TreeNode(this.getName() + "_root", RelationTypeEnum.ROOT);
			n.setIsKeyNode(true);
			for (int i=0; i<this.getChildOperators().size(); i++){
				AbstractOperator ao = this.getChildAt(i);
				TreeNode childKeyNode = ao.getApt().getKeyNode();
				if (childKeyNode != null){
					int childKeyNodeId = childKeyNode.getNodeId();
					
					TreeNode c = new TreeNode(childKeyNodeId, RelationTypeEnum.PARENT_CHILD);
					
					// Especificao de cardinalidade do nodo que est sendo consultado para determinao
					// se o tipo de consulta  por FOR ou LET
					if (ao.getName().equals("Select"))
						c.setMatchSpec(childKeyNode.getMatchSpec());
					else if ((ao.getName().equals("Join")) || (ao.getName().equals("Union")))
						c.setMatchSpec(childKeyNode.getChild(0).getMatchSpec());
					
					n.addChild(c);
				}
				else{
					throw new AlgebraParserException("No Key Node found to build UnionJoinOperator APT");
				}
			}
			this.apt.setAptNode(n);
		}
	}	
}
