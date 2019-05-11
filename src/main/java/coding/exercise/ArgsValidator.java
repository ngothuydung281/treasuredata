package coding.exercise;

public class ArgsValidator
{
    public boolean isDigitOnly(String str, String label)
    {
        if (!str.matches("[0-9]+"))
        {
            MyExercise.errorList.add(String.format("ERROR: %s '%s' is not valid. It should be digit only", label, str));
            return false;
        }
        return true;
    }

    public void checkTimeValid(String minTime, String maxTime)
    {
        if (Long.parseLong(maxTime) < Long.parseLong(minTime))
        {
            MyExercise.errorList.add(String.format("ERROR: Maximum time '%s' must be equal or greater than minimum time '%s'", maxTime, minTime));
        }
    }
}
