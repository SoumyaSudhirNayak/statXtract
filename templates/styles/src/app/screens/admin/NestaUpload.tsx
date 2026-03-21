import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function NestaUpload() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [formData, setFormData] = useState({
    targetSchema: "",
    dataType: "",
  });
  const [file, setFile] = useState<File | null>(null);
  const [progress, setProgress] = useState(0);
  const [isMainHovered, setIsMainHovered] = useState(false);
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);

  const handleUpload = () => {
    setProgress(0);
    const interval = setInterval(() => {
      setProgress((prev) => {
        if (prev >= 100) {
          clearInterval(interval);
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
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Nesstar Upload</h1>
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
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Target Schema</label>
                <input
                  type="text"
                  value={formData.targetSchema}
                  onChange={(e) => setFormData({ ...formData, targetSchema: e.target.value })}
                  onMouseEnter={() => setHoveredBox('target-schema')}
                  onMouseLeave={() => setHoveredBox(isMainHovered ? 'main' : null)}
                  placeholder="Enter target schema name"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'target-schema' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'target-schema' ? '0 0 20px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">File Upload</label>
                <input
                  type="file"
                  onChange={(e) => setFile(e.target.files?.[0] || null)}
                  onMouseEnter={() => setHoveredBox('file-upload')}
                  onMouseLeave={() => setHoveredBox(isMainHovered ? 'main' : null)}
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
                  style={{
                    borderColor: hoveredBox === 'file-upload' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'file-upload' ? '0 0 20px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Data Type</label>
                <select
                  value={formData.dataType}
                  onChange={(e) => setFormData({ ...formData, dataType: e.target.value })}
                  onMouseEnter={() => setHoveredBox('data-type')}
                  onMouseLeave={() => setHoveredBox(isMainHovered ? 'main' : null)}
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'data-type' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'data-type' ? '0 0 20px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <option value="">Select data type...</option>
                  <option value="survey">Survey Data</option>
                  <option value="microdata">Microdata</option>
                  <option value="aggregate">Aggregate Data</option>
                </select>
              </div>

              {progress > 0 && (
                <div className="mt-6">
                  <div className="flex justify-between text-lg text-[#6B7280] mb-3">
                    <span className="font-bold">Upload Progress</span>
                    <span className="font-bold">{progress}%</span>
                  </div>
                  <div className="w-full bg-[#E5E7EB] rounded-full h-4 border-2 border-[#2E5BBA] overflow-hidden">
                    <div
                      className="bg-[#2E7D32] h-full rounded-full transition-all duration-300"
                      style={{ width: `${progress}%` }}
                    />
                  </div>
                </div>
              )}

              <div className="flex gap-6 mt-10">
                <button
                  onClick={handleUpload}
                  className="flex-1 bg-[#2E5BBA] text-white py-5 rounded-xl transition-all duration-300 hover:bg-[#16324F] border-2 border-[#2E5BBA] hover:border-[#F4A300] cursor-pointer text-2xl font-black shadow-lg"
                >
                  Upload
                </button>
                <button
                  onClick={() => navigate("/admin/dashboard")}
                  className="flex-1 bg-white text-[#1C1C1C] py-5 rounded-xl border-2 border-[#D1D5DB] transition-all duration-300 hover:bg-[#F5F7FA] cursor-pointer text-2xl font-black"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
