import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function PrivateOrgVerification() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    companyName: "",
    designation: "",
    employeeId: "",
    workEmail: "",
    address: "",
  });
  const [file, setFile] = useState<File | null>(null);
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const [isContainerHovered, setIsContainerHovered] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const userData = JSON.parse(sessionStorage.getItem("userData") || "{}");
    sessionStorage.setItem("userData", JSON.stringify({ ...userData, ...formData, orgType: "Private" }));
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
            Private Organization Verification
          </h2>

          <form 
            ref={containerRef}
            onSubmit={handleSubmit} 
            onMouseEnter={() => setIsContainerHovered(true)}
            onMouseLeave={() => setIsContainerHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 relative overflow-hidden shadow-2xl"
            style={{
              borderColor: isContainerHovered ? '#7B1FA2' : '#D1D5DB',
              boxShadow: isContainerHovered 
                ? '0 30px 70px rgba(123, 31, 162, 0.25)'
                : 'none',
            }}
          >
            <div className="space-y-8 relative z-10">
              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Company Name</label>
                <input
                  type="text"
                  value={formData.companyName}
                  onChange={(e) => setFormData({ ...formData, companyName: e.target.value })}
                  onMouseEnter={() => setHoveredInput('companyName')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  placeholder="e.g. Acme Corp, Global Tech"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'companyName' ? '#7B1FA2' : '#D1D5DB',
                    boxShadow: hoveredInput === 'companyName' ? '0 0 15px rgba(123, 31, 162, 0.2)' : 'none'
                  }}
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Designation</label>
                  <input
                    type="text"
                    value={formData.designation}
                    onChange={(e) => setFormData({ ...formData, designation: e.target.value })}
                    onMouseEnter={() => setHoveredInput('designation')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                    style={{
                      borderColor: hoveredInput === 'designation' ? '#1976D2' : '#D1D5DB',
                      boxShadow: hoveredInput === 'designation' ? '0 0 15px rgba(25, 118, 210, 0.2)' : 'none'
                    }}
                  />
                </div>
                <div>
                  <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Employee ID</label>
                  <input
                    type="text"
                    value={formData.employeeId}
                    onChange={(e) => setFormData({ ...formData, employeeId: e.target.value })}
                    onMouseEnter={() => setHoveredInput('employeeId')}
                    onMouseLeave={() => setHoveredInput(null)}
                    required
                    className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                    style={{
                      borderColor: hoveredInput === 'employeeId' ? '#D32F2F' : '#D1D5DB',
                      boxShadow: hoveredInput === 'employeeId' ? '0 0 15px rgba(211, 47, 47, 0.2)' : 'none'
                    }}
                  />
                </div>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Work Email</label>
                <input
                  type="email"
                  value={formData.workEmail}
                  onChange={(e) => setFormData({ ...formData, workEmail: e.target.value })}
                  onMouseEnter={() => setHoveredInput('workEmail')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'workEmail' ? '#2E7D32' : '#D1D5DB',
                    boxShadow: hoveredInput === 'workEmail' ? '0 0 15px rgba(46, 125, 50, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Company Address</label>
                <input
                  type="text"
                  value={formData.address}
                  onChange={(e) => setFormData({ ...formData, address: e.target.value })}
                  onMouseEnter={() => setHoveredInput('address')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'address' ? '#F57C00' : '#D1D5DB',
                    boxShadow: hoveredInput === 'address' ? '0 0 15px rgba(245, 124, 0, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-3 text-xl font-bold">Upload Employment Proof (PDF/Image)</label>
                <input
                  type="file"
                  onChange={(e) => setFile(e.target.files?.[0] || null)}
                  onMouseEnter={() => setHoveredInput('file')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 border-dashed rounded-xl focus:outline-none bg-white transition-all duration-300 text-xl cursor-pointer font-bold"
                  style={{
                    borderColor: hoveredInput === 'file' ? '#D32F2F' : '#D1D5DB',
                    boxShadow: hoveredInput === 'file' ? '0 0 15px rgba(211, 47, 47, 0.2)' : 'none'
                  }}
                />
              </div>

              <button
                type="submit"
                onMouseEnter={() => setHoveredInput('submit')}
                onMouseLeave={() => setHoveredInput(null)}
                className="lift-on-hover w-full bg-[#7B1FA2] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg mt-6"
                style={{
                  borderColor: hoveredInput === 'submit' ? '#F4A300' : '#7B1FA2',
                  boxShadow: hoveredInput === 'submit' ? '0 15px 40px rgba(244, 163, 0, 0.5)' : 'none'
                }}
              >
                Complete Organization Verification
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
