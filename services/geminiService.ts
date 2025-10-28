
import { GoogleGenAI } from "@google/genai";

if (!process.env.API_KEY) {
  console.warn("API_KEY environment variable not set. Gemini API calls will fail.");
}

const ai = new GoogleGenAI({ apiKey: process.env.API_KEY! });

export const analyzeWithGemini = async (prompt: string, isThinkingMode: boolean): Promise<string> => {
  try {
    // FIX: Use 'gemini-flash-lite-latest' for the lite model as per guidelines.
    const modelName = isThinkingMode ? 'gemini-2.5-pro' : 'gemini-flash-lite-latest';
    
    console.log(`Using model: ${modelName}. Thinking Mode: ${isThinkingMode}`);

    const response = await ai.models.generateContent({
        model: modelName,
        contents: prompt,
        ...(isThinkingMode && {
            config: {
                thinkingConfig: { thinkingBudget: 32768 }
            }
        })
    });

    return response.text;
  } catch (error) {
    console.error("Error calling Gemini API:", error);
    if (error instanceof Error) {
        return `An error occurred while communicating with the Gemini API: ${error.message}`;
    }
    return "An unknown error occurred while communicating with the Gemini API.";
  }
};
