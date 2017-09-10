package pagerank;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;

/*
    Class represents the Node structure in Graph.
    Stores pagename, pagerank and its adjacency list
*/
public class GraphVertexData {

    String PageName;
    double PageRank;
    ArrayList<String> AdjacencyList;

    public GraphVertexData(){
        PageName = "";
        PageRank = -1;
        AdjacencyList = new ArrayList<String>();
    }

    /*
        Converts node to a text form (for writing to file)
        Text representation: PageName~PageRank~[page1~page~page3...]
    */
    @Override
    public String toString(){

        StringBuilder adjList = new StringBuilder();
        adjList.append("[");
        adjList.append(StringUtils.join(this.AdjacencyList, "~"));
        adjList.append("]");

        return(this.PageName + "~" + this.PageRank + "~" + adjList);
    }

    /*
        Converts text representation to node representation
    */
    public GraphVertexData ConvertToVertexRep(String value){
        String[] array = value.split("~", 3);

        GraphVertexData vertex = new GraphVertexData();
        vertex.PageName = array[0];
        vertex.PageRank = Double.parseDouble(array[1]);
        vertex.AdjacencyList = new ArrayList<String>();

        if(!array[2].equals("[]")){
            array[2] = array[2].replace("[", "");
            array[2] = array[2].replace("]", "");
            String[] pageNames = array[2].split("~");

            for (String page : pageNames) {
                vertex.AdjacencyList.add(page);
            }
        }

        return vertex;
    }
}
