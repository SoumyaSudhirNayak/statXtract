import { useNavigate } from "react-router";
import { ArrowLeft } from "lucide-react";

interface BackButtonProps {
  to?: string;
  className?: string;
}

export function BackButton({ to, className = "" }: BackButtonProps) {
  const navigate = useNavigate();

  const handleClick = () => {
    if (to) {
      navigate(to);
    } else {
      navigate(-1);
    }
  };

  return (
    <button
      onClick={handleClick}
      className={`
        inline-flex items-center gap-1.5 px-3 py-1.5 text-sm
        bg-white border-2 border-[#2E5BBA] rounded-lg 
        text-[#1F3A5F] transition-all duration-200
        hover:bg-[#2E5BBA] hover:text-white 
        hover:-translate-y-1
        ${className}
      `}
      style={{
        boxShadow: 'none',
      }}
      aria-label="Go back"
    >
      <ArrowLeft className="w-4 h-4" />
      <span className="font-medium">Back</span>
    </button>
  );
}