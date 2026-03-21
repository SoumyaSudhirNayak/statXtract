import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { Upload as UploadIcon } from "lucide-react";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function UploadDataset() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [category, setCategory] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [isMainHovered, setIsMainHovered] = useState(false);
  const [isGuidelinesHovered, setIsGuidelinesHovered] = useState(false);
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);

  const handleUpload = () => {
    if (!file) return;
    
    setUploading(true);
    setProgress(0);
    
    const interval = setInterval(() => {
      setProgress((prev) => {
        if (prev >= 100) {
          clearInterval(interval);
          setUploading(false);
          return 100;
        }
        return prev + 10;
      });
    }, 200);
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1400px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Upload Dataset</h1>
            <div className="w-[80px]"></div>
          </div>

          <div 
            onMouseEnter={() => setIsMainHovered(true)}
            onMouseLeave={() => setIsMainHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-12 transition-all duration-300 shadow-xl"
            style={{
              borderColor: isMainHovered ? '#2E5BBA' : '#D1D5DB',
              boxShadow: isMainHovered ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
            }}
          >
            <div className="space-y-10">
              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Select Dataset Category</label>
                <select
                  value={category}
                  onChange={(e) => setCategory(e.target.value)}
                  onMouseEnter={() => setHoveredInput('category-select')}
                  onMouseLeave={() => setHoveredInput(null)}
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl"
                  style={{
                    borderColor: hoveredInput === 'category-select' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'category-select' ? '0 0 20px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <option value="">Choose a category...</option>
                  <option value="census">Census Data</option>
                  <option value="economic">Economic Indicators</option>
                  <option value="health">Health Statistics</option>
                  <option value="education">Education Data</option>
                </select>
              </div>

              <div 
                onMouseEnter={() => setIsGuidelinesHovered(true)}
                onMouseLeave={() => setIsGuidelinesHovered(false)}
                className="bg-[#F5F7FA] border-2 rounded-xl p-8 transition-all duration-300"
                style={{
                  borderColor: isGuidelinesHovered ? '#2E7D32' : '#D1D5DB',
                  boxShadow: isGuidelinesHovered ? '0 0 20px rgba(46, 125, 50, 0.2)' : 'none'
                }}
              >
                <h4 className="text-2xl text-[#1C1C1C] mb-4 font-bold" style={{ fontFamily: 'var(--font-head)' }}>Guidelines</h4>
                <ul className="text-[#6B7280] text-xl space-y-3 m-0 pl-8 list-disc">
                  <li>File must be in ZIP format (can contain CSV, Excel, or other data files)</li>
                  <li>Maximum file size: 500MB</li>
                  <li>Ensure all files are properly formatted</li>
                  <li>Include documentation if available</li>
                </ul>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Upload ZIP File</label>
                <div 
                  onMouseEnter={() => setHoveredInput('upload')}
                  onMouseLeave={() => setHoveredInput(null)}
                  className="lift-on-hover border-2 border-dashed rounded-xl p-16 text-center transition-all duration-300 bg-white/50"
                  style={{
                    borderColor: hoveredInput === 'upload' ? '#F4A300' : '#D1D5DB',
                    boxShadow: hoveredInput === 'upload' ? '0 0 20px rgba(244, 163, 0, 0.2)' : 'none'
                  }}
                >
                  <UploadIcon className="w-20 h-20 text-[#6B7280] mx-auto mb-6" />
                  <input
                    type="file"
                    accept="*"
                    onChange={(e) => setFile(e.target.files?.[0] || null)}
                    className="hidden"
                    id="file-upload"
                  />
                  <label
                    htmlFor="file-upload"
                    className="text-[#2E5BBA] cursor-pointer hover:underline text-2xl font-medium"
                  >
                    {file ? file.name : "Browse or drag & drop ZIP file (any file type accepted)"}
                  </label>
                </div>
              </div>

              {(uploading || progress > 0) && (
                <div className="mt-6">
                  <div className="flex justify-between text-lg text-[#6B7280] mb-3">
                    <span className="font-bold">Progress</span>
                    <span className="font-bold">{progress}%</span>
                  </div>
                  <div className="w-full bg-[#E5E7EB] rounded-full h-4 border-2 border-[#2E5BBA] overflow-hidden">
                    <div
                      className="bg-[#2E7D32] h-full transition-all duration-300"
                      style={{ width: `${progress}%` }}
                    />
                  </div>
                </div>
              )}

              <button
                onClick={handleUpload}
                disabled={!file || !category || uploading}
                className="lift-on-hover w-full bg-[#2E5BBA] text-white py-5 rounded-xl transition-all duration-300 hover:bg-[#16324F] border-2 border-[#2E5BBA] hover:border-[#F4A300] cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed text-2xl font-bold shadow-lg mt-6"
              >
                Upload Dataset
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}