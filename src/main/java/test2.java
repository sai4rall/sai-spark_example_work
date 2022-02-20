import com.spark.transformations.config.QuollMapConstants;
import org.apache.hadoop.shaded.com.google.common.base.CharMatcher;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test2 {
    public static void main(String[] args) {
        String incChar = "" + CharMatcher.digit().retainFrom("(LTE33)");
        System.out.println(incChar);
        //  System.out.println("ooooooo"+genEGNodeBName("null", "(TESTTTTTT(LTE3)cds", 500000, ""));
    }
}
/*

def genNodeBName(site, nodeCode):
    try:
        pattern = re.compile("\([1-9]\)")
        m = pattern.search(site)
        if m:
            site = site[m.start():]
            incChar = ''.join(filter(str.isdigit, site))
            return nodeCode + incChar

        pattern = re.compile("\([1-9][0-9]\)")
        m = pattern.search(site)
        if m:
            site = site[m.start():]
            incChar = ''.join(filter(str.isdigit, site))
            return nodeCode + ranNumberingMap.value[incChar]

        # otherwise assume '1'
        return nodeCode + '1'

    except:
        return None
 */





