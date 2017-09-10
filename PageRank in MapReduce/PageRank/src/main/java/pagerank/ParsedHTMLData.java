package pagerank;

import java.util.ArrayList;
import java.util.List;

/*
    Class is used by Bz2WikiParser to return the page name and list having
    linked pages
*/
public class ParsedHTMLData{

    String PageName;
    ArrayList<String> LinkedPageNames;

    public ParsedHTMLData(){
    }

    public ParsedHTMLData(String pageName, ArrayList<String> linkedPageNames){
        PageName = pageName;
        LinkedPageNames = linkedPageNames;
    }
}
