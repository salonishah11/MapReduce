package pagerank;

import java.util.ArrayList;
import java.util.List;

/*
    Class is used by Bz2WikiParser to return the page name and list having
    linked pages
*/
public class ParsedHTMLData
{
    String PageName;
    List<String> LinkedPageNames;

    public ParsedHTMLData() {}

    public ParsedHTMLData(String pageName, List<String> linkedPageNames)
    {
        PageName = pageName;
        LinkedPageNames = linkedPageNames;
    }

    public String toString()
    {
        return PageName + "~" + LinkedPageNames.toString();
    }
}