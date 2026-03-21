import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function NadaImport() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [formData, setFormData] = useState({
    apiKey: "",
    datasetId: "",
    targetSchema: "",
  });
  const [hoveredBox, setHoveredBox] = useState<string | null>(null);

  const handleDownloadIngest = () => {
    alert("Download & Ingest process initiated");
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
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>NADA Import</h1>
            <div className="w-[80px]"></div>
          </div>

          <div 
            onMouseEnter={() => setHoveredBox('main')}
            onMouseLeave={() => setHoveredBox(null)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-12 transition-all duration-300 shadow-xl"
            style={{
              borderColor: hoveredBox === 'main' || hoveredBox?.includes('-') ? '#2E5BBA' : '#D1D5DB',
              boxShadow: hoveredBox === 'main' || hoveredBox?.includes('-') ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
            }}
          >
            <div className="space-y-10">
              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">API Key</label>
                <input
                  type="password"
                  value={formData.apiKey}
                  onChange={(e) => setFormData({ ...formData, apiKey: e.target.value })}
                  onMouseEnter={() => setHoveredBox('api-key')}
                  onMouseLeave={() => setHoveredBox('main')}
                  placeholder="Enter NADA API key"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'api-key' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'api-key' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <button 
                  onMouseEnter={() => setHoveredBox('browse-btn')}
                  onMouseLeave={() => setHoveredBox('main')}
                  className="lift-on-hover bg-[#F5F7FA] text-[#2E5BBA] px-10 py-4 rounded-xl border-2 cursor-pointer transition-all duration-300 text-xl font-black shadow-md"
                  style={{
                    borderColor: hoveredBox === 'browse-btn' ? '#2E7D32' : '#D1D5DB',
                    boxShadow: hoveredBox === 'browse-btn' ? '0 10px 25px rgba(46, 125, 50, 0.2)' : 'none'
                  }}
                >
                  Browse Dataset
                </button>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Dataset ID</label>
                <input
                  type="text"
                  value={formData.datasetId}
                  onChange={(e) => setFormData({ ...formData, datasetId: e.target.value })}
                  onMouseEnter={() => setHoveredBox('dataset-id')}
                  onMouseLeave={() => setHoveredBox('main')}
                  placeholder="Enter dataset ID"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'dataset-id' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'dataset-id' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Target Schema</label>
                <select
                  value={formData.targetSchema}
                  onChange={(e) => setFormData({ ...formData, targetSchema: e.target.value })}
                  onMouseEnter={() => setHoveredBox('target-schema')}
                  onMouseLeave={() => setHoveredBox('main')}
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredBox === 'target-schema' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredBox === 'target-schema' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                >
                  <option value="">Select target schema...</option>
                  <option value="public">Public</option>
                  <option value="census">Census</option>
                  <option value="survey">Survey</option>
                </select>
              </div>

              <div className="flex gap-6 mt-10">
                <button
                  onClick={handleDownloadIngest}
                  className="flex-1 bg-[#2E5BBA] text-white py-5 rounded-xl transition-all duration-300 hover:bg-[#16324F] border-2 border-[#2E5BBA] hover:border-[#F4A300] cursor-pointer text-2xl font-black shadow-lg"
                >
                  Download & Ingest
                </button>
                <button
                  onClick={() => navigate("/admin/dashboard")}
                  className="flex-1 bg-white text-[#1C1C1C] py-5 rounded-xl border-2 border-[#D1D5DB] transition-all duration-300 hover:bg-[#F5F7FA] cursor-pointer text-2xl font-black"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}