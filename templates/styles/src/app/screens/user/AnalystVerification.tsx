import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function AnalystVerification() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    portfolioLink: "",
    experience: "",
    areaOfAnalysis: "",
  });
  const [file, setFile] = useState<File | null>(null);
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const [isContainerHovered, setIsContainerHovered] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const userData = JSON.parse(sessionStorage.getItem("userData") || "{}");
    sessionStorage.setItem("userData", JSON.stringify({ ...userData, ...formData, orgType: "Analyst" }));
    navigate("/user/dashboard");
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <div className="bg-[#1F3A5F] text-white px-10 py-6 shadow-xl relative z-50">
          <div className="max-w-[1800px] mx-auto flex justify-between items-center">
            <h1 
              className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
              style={{ fontFamily: 'var(--font-head)', color: 'white' }}
              onClick={() => navigate("/")}
            >
              STATXTRACT
            </h1>
          </div>
        </div>

        <div className="max-w-[1000px] mx-auto px-6 py-20">
          <div className="mb-10">
            <BackButton to="/user/organization-type" />
          </div>
          
          <h2 className="text-5xl text-[#1C1C1C] mb-12 text-center font-black" style={{ fontFamily: 'var(--font-head)' }}>
            Analyst Verification
          </h2>

          <form 
            ref={containerRef}
            onSubmit={handleSubmit} 
            onMouseEnter={() => setIsContainerHovered(true)}
            onMouseLeave={() => setIsContainerHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 relative overflow-hidden shadow-2xl"
            style={{
              borderColor: isContainerHovered ? '#F57C00' : '#D1D5DB',
              boxShadow: isContainerHovered 
                ? '0 30px 70px rgba(245, 124, 0, 0.25)'
                : 'none',
            }}
          >
            <div className="space-y-8 relative z-10">
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Portfolio Link (LinkedIn/Website)</label>
                <input
                  type="url"
                  value={formData.portfolioLink}
                  onChange={(e) => setFormData({ ...formData, portfolioLink: e.target.value })}
                  onMouseEnter={() => setHoveredInput('portfolioLink')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  placeholder="https://linkedin.com/in/username"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'portfolioLink' ? '#F57C00' : '#D1D5DB',
                    boxShadow: hoveredInput === 'portfolioLink' ? '0 0 15px rgba(245, 124, 0, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Years of Experience</label>
                <input
                  type="text"
                  value={formData.experience}
                  onChange={(e) => setFormData({ ...formData, experience: e.target.value })}
                  onMouseEnter={() => setHoveredInput('experience')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  placeholder="e.g. 5 Years"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'experience' ? '#1976D2' : '#D1D5DB',
                    boxShadow: hoveredInput === 'experience' ? '0 0 15px rgba(25, 118, 210, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Primary Area of Analysis</label>
                <input
                  type="text"
                  value={formData.areaOfAnalysis}
                  onChange={(e) => setFormData({ ...formData, areaOfAnalysis: e.target.value })}
                  onMouseEnter={() => setHoveredInput('areaOfAnalysis')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  placeholder="e.g. Financial Data, Market Research"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'areaOfAnalysis' ? '#D32F2F' : '#D1D5DB',
                    boxShadow: hoveredInput === 'areaOfAnalysis' ? '0 0 15px rgba(211, 47, 47, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Upload Certification/Credentials (PDF/Image)</label>
                <input
                  type="file"
                  onChange={(e) => setFile(e.target.files?.[0] || null)}
                  onMouseEnter={() => setHoveredInput('file')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 border-dashed rounded-xl focus:outline-none bg-white transition-all duration-300 text-xl cursor-pointer font-bold"
                  style={{
                    borderColor: hoveredInput === 'file' ? '#2E7D32' : '#D1D5DB',
                    boxShadow: hoveredInput === 'file' ? '0 0 15px rgba(46, 125, 50, 0.2)' : 'none'
                  }}
                />
              </div>

              <button
                type="submit"
                onMouseEnter={() => setHoveredInput('submit')}
                onMouseLeave={() => setHoveredInput(null)}
                className="lift-on-hover w-full bg-[#F57C00] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg mt-6"
                style={{
                  borderColor: hoveredInput === 'submit' ? '#F4A300' : '#F57C00',
                  boxShadow: hoveredInput === 'submit' ? '0 15px 40px rgba(244, 163, 0, 0.5)' : 'none'
                }}
              >
                Complete Analyst Verification
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
