package preparse;

public class WebLogPreParser {

    public static PreParsedLog parse(String line){
        if (line.startsWith("#")){
            return null;
        }else{
            PreParsedLog preParsedLog=new PreParsedLog();
            String[] temps=line.split(" ");
            preParsedLog.setServerTime(temps[0]+" "+temps[1]);
            preParsedLog.setServerIp(temps[2]);
            preParsedLog.setMethod(temps[3]);
            preParsedLog.setUriStem(temps[4]);
            String queryString=temps[5];
            preParsedLog.setQueryString(queryString);
            String[] queryTemps=queryString.split("&");
            String command=queryTemps[1].split("=")[1];
            preParsedLog.setCommand(command);
            String profileIdStr=queryTemps[2].split("=")[1];
            preParsedLog.setProfileId(getProfileId(profileIdStr));
            preParsedLog.setServerPort(Integer.parseInt(temps[6]));
            preParsedLog.setClientIp(temps[8]);
            preParsedLog.setUserAgent(temps[9].replaceAll("\\+"," "));

            String timeTemp=preParsedLog.getServerTime().replaceAll("-","");
            preParsedLog.setYear(Integer.valueOf(timeTemp.substring(0,4)));
            preParsedLog.setMonth(Integer.valueOf(timeTemp.substring(0,6)));
            preParsedLog.setDay(Integer.valueOf(timeTemp.substring(0,8)));
            return preParsedLog;
        }
    }
    private  static int getProfileId(String profileIdStr){
        return Integer.valueOf(profileIdStr.split("-")[1]);
    }
}
