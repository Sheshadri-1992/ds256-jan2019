package in.ds256.Assignment2;

public class GFG {

	public static String removeWord(String string, String word) 
    { 
  
        // Check if the word is present in string 
        // If found, remove it using removeAll() 
        if (string.contains(word)) { 
  
            // To cover the case 
            // if the word is at the 
            // beginning of the string 
            // or anywhere in the middle 
            String tempWord = word + " "; 
            
            tempWord = "\\b" + word+ "\\b";
            System.out.println("The word is "+tempWord);
            string = string.replaceAll(tempWord, "");            
            
        } 
  
        // Return the resultant string 
        return string; 
    } 
  
    public static void main(String args[]) 
    { 
  
        // Test Case 1: 
        // If the word is in the middle 
        String string1 = "Geeks for Geeks."; 
        String word1 = "Geeks"; 
  
        // Test Case 2: 
        // If the word is at the beginning 
        String string2 = "for Geeks Geeks."; 
        String word2 = "for"; 
  
        // Test Case 3: 
        // If the word is at the end 
        String string3 = "Geeks Geeks for."; 
        String word3 = "for"; 
  
        // Test Case 4: 
        // If the word is not present 
        String string4 = "A computer Science Portal."; 
        String word4 = "Geeks"; 
  
        // Test case 1 
        System.out.println("String: " + string1 
                           + "\nWord: " + word1 
                           + "\nResult String: "
                           + removeWord(string1, word1)); 
  
        // Test case 2 
//        System.out.println("\nString: " + string2 
//                           + "\nWord: " + word2 
//                           + "\nResult String: "
//                           + removeWord(string2, word2)); 
//  
//        // Test case 3 
//        System.out.println("\nString: " + string3 
//                           + "\nWord: " + word3 
//                           + "\nResult String: "
//                           + removeWord(string3, word3)); 
//  
//        // Test case 4 
//        System.out.println("\nString: " + string4 
//                           + "\nWord: " + word4 
//                           + "\nResult String: "
//                           + removeWord(string4, word4)); 
    } 

}
